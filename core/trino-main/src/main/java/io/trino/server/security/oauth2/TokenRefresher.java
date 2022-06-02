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

import io.trino.server.security.oauth2.OAuth2Client.Response;
import io.trino.server.security.oauth2.TokenPairSerializer.TokenPair;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;

import static io.trino.server.security.oauth2.OAuth2TokenExchange.hashAuthId;
import static java.util.Objects.requireNonNull;

public class TokenRefresher
{
    private final TokenPairSerializer tokenAssembler;
    private final OAuth2TokenHandler tokenHandler;
    private final OAuth2Client oAuth2Client;
    private final Executor refreshExecutor;

    public TokenRefresher(TokenPairSerializer tokenAssembler, OAuth2TokenHandler tokenHandler, OAuth2Client oAuth2Client, Executor refreshExecutor)
    {
        this.tokenAssembler = requireNonNull(tokenAssembler, "tokenAssembler is null");
        this.tokenHandler = requireNonNull(tokenHandler, "tokenHandler is null");
        this.oAuth2Client = requireNonNull(oAuth2Client, "oAuth2Client is null");
        this.refreshExecutor = requireNonNull(refreshExecutor, "refreshExecutor is null");
    }

    public Optional<UUID> refreshToken(TokenPair tokenPair)
    {
        requireNonNull(tokenPair, "tokenPair is null");

        Optional<String> refreshToken = tokenPair.getRefreshToken();
        if (refreshToken.isPresent()) {
            UUID refreshingId = UUID.randomUUID();
            refreshExecutor.execute(() -> refreshToken(refreshToken.get(), refreshingId));
            return Optional.of(refreshingId);
        }
        return Optional.empty();
    }

    private void refreshToken(String refreshToken, UUID refreshingId)
    {
        try {
            Response response = oAuth2Client.refreshTokens(refreshToken);
            String serializedToken = tokenAssembler.serialize(TokenPair.fromOAuth2Response(response));
            tokenHandler.setAccessToken(hashAuthId(refreshingId), serializedToken);
        }
        catch (ChallengeFailedException e) {
            tokenHandler.setTokenExchangeError(hashAuthId(refreshingId), "Token refreshing has failed: " + e.getMessage());
        }
    }
}
