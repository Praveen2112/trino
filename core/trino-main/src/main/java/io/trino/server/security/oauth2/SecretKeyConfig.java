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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;

import static com.google.common.base.Strings.isNullOrEmpty;

public class SecretKeyConfig
{
    private SecretKey secretKey;

    @Config("http-server.authentication.oauth2.refresh-tokens.secret-key")
    @ConfigDescription("Base64 encoded secret key used to encrypt generated token")
    @ConfigSecuritySensitive
    public SecretKeyConfig setSecretKey(String key)
    {
        if (isNullOrEmpty(key)) {
            return this;
        }

        secretKey = Keys.hmacShaKeyFor(Decoders.BASE64.decode(key));
        return this;
    }

    public SecretKey getSecretKey()
    {
        return secretKey;
    }
}
