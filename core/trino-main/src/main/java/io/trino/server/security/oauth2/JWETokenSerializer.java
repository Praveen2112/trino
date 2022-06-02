/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.security.oauth2;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.KeyLengthException;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.AESDecrypter;
import com.nimbusds.jose.crypto.AESEncrypter;
import io.airlift.units.Duration;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.CompressionCodec;
import io.jsonwebtoken.CompressionException;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.JwtParser;

import javax.crypto.SecretKey;

import java.text.ParseException;
import java.time.Clock;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JWETokenSerializer
        implements TokenPairSerializer
{
    private static final CompressionCodec COMPRESSION_CODEC = new ZstdCodec();
    private static final String ACCESS_TOKEN_KEY = "access_token";
    private static final String REFRESH_TOKEN_KEY = "refresh_token";
    private final OAuth2Client client;
    private final Clock clock;
    private final String issuer;
    private final String audience;
    private final Duration accessTokenExpiration;
    private final JwtParser parser;
    private final AESEncrypter jweEncrypter;
    private final AESDecrypter jweDecrypter;
    private final String principalField;

    public JWETokenSerializer(
            JweSecretKeysProvider keysProvider,
            OAuth2Client client,
            String issuer,
            String audience,
            String principalField,
            Clock clock,
            Duration accessTokenExpiration,
            Duration refreshWindow)
            throws KeyLengthException
    {
        SecretKey secretKey = requireNonNull(keysProvider, "keysProvider is null").getSecretKey();
        this.jweEncrypter = new AESEncrypter(secretKey);
        this.jweDecrypter = new AESDecrypter(secretKey);
        this.client = requireNonNull(client, "client is null");
        this.issuer = requireNonNull(issuer, "issuer is null");
        this.principalField = requireNonNull(principalField, "principalField is null");
        this.audience = requireNonNull(audience, "issuer is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.accessTokenExpiration = requireNonNull(accessTokenExpiration, "accessTokenExpiration is null");
        requireNonNull(refreshWindow, "refreshWindow is null");

        this.parser = newJwtParserBuilder()
                .setClock(() -> Date.from(clock.instant()))
                .requireIssuer(this.issuer)
                .requireAudience(this.audience)
                .setCompressionCodecResolver(JWETokenSerializer::resolveCompressionCodec)
                .setAllowedClockSkewSeconds(refreshWindow.roundTo(SECONDS))
                .build();
    }

    @Override
    public TokenPair deserialize(String token)
    {
        requireNonNull(token, "token is null");

        try {
            JWEObject jwe = JWEObject.parse(token);
            jwe.decrypt(jweDecrypter);
            Claims claims = parser.parseClaimsJwt(jwe.getPayload().toString()).getBody();
            return TokenPair.accessAndRefreshTokens(
                    claims.get(ACCESS_TOKEN_KEY, String.class),
                    claims.get(REFRESH_TOKEN_KEY, String.class));
        }
        catch (ExpiredJwtException ex) {
            throw new IllegalArgumentException("Token has timed out", ex);
        }
        catch (ParseException ex) {
            throw new IllegalArgumentException("Malformed jwt token", ex);
        }
        catch (JOSEException ex) {
            throw new IllegalArgumentException("Decryption failed", ex);
        }
    }

    @Override
    public String serialize(TokenPair tokenPair)
    {
        requireNonNull(tokenPair, "tokenPair is null");

        Optional<Map<String, Object>> accessTokenClaims = client.getClaims(tokenPair.getAccessToken());
        JwtBuilder jwt = newJwtBuilder()
                .setExpiration(Date.from(clock.instant().plusMillis(accessTokenExpiration.toMillis())))
                .claim(principalField, accessTokenClaims.map(claims -> claims.get(principalField).toString()).orElse("Unknown"))
                .setAudience(audience)
                .setIssuer(issuer)
                .claim(ACCESS_TOKEN_KEY, tokenPair.getAccessToken())
                .claim(REFRESH_TOKEN_KEY, tokenPair.getRefreshToken().orElseThrow(JWETokenSerializer::throwExceptionForNonExistingRefreshToken))
                .compressWith(COMPRESSION_CODEC);

        try {
            JWEObject jwe = new JWEObject(
                    new JWEHeader(JweSecretKeysProvider.ALGORITHM, JweSecretKeysProvider.ENCRYPTION_METHOD),
                    new Payload(jwt.compact()));
            jwe.encrypt(jweEncrypter);
            return jwe.serialize();
        }
        catch (JOSEException ex) {
            throw new IllegalStateException("Encryption failed", ex);
        }
    }

    private static RuntimeException throwExceptionForNonExistingRefreshToken()
    {
        return new IllegalStateException("Expected refresh token to be present. Please check your identity provider setup, or disable refresh tokens");
    }

    private static CompressionCodec resolveCompressionCodec(Header header)
            throws CompressionException
    {
        if (header.getCompressionAlgorithm() != null) {
            checkState(header.getCompressionAlgorithm().equals(ZstdCodec.CODEC_NAME), "Unknown codec '%s' used for token compression", header.getCompressionAlgorithm());
            return COMPRESSION_CODEC;
        }
        return null;
    }
}
