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

import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JWEAlgorithm;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.inject.Inject;

import java.security.NoSuchAlgorithmException;

import static java.util.Objects.requireNonNull;

public class JweSecretKeysProvider
{
    public static final JWEAlgorithm ALGORITHM = JWEAlgorithm.A256KW;
    public static final EncryptionMethod ENCRYPTION_METHOD = EncryptionMethod.A256CBC_HS512;
    private final SecretKey secretKey;

    @Inject
    JweSecretKeysProvider(SecretKeyConfig config)
            throws NoSuchAlgorithmException
    {
        requireNonNull(config, "config is null");

        this.secretKey = createKey(config);
    }

    private static SecretKey createKey(SecretKeyConfig config)
            throws NoSuchAlgorithmException
    {
        SecretKey signingKey = config.getSecretKey();
        if (signingKey == null) {
            KeyGenerator generator = KeyGenerator.getInstance("AES");
            generator.init(256);
            return generator.generateKey();
        }
        return signingKey;
    }

    public SecretKey getSecretKey()
    {
        return secretKey;
    }
}
