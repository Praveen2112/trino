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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.NoSuchAlgorithmException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.jsonwebtoken.io.Encoders.BASE64;

public class TestSecretKeyConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SecretKeyConfig.class)
                .setSecretKey(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        String encodedBase64SecretKey = BASE64.encode(generateKey());

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.oauth2.refresh-tokens.secret-key", encodedBase64SecretKey)
                .buildOrThrow();

        SecretKeyConfig expected = new SecretKeyConfig()
                .setSecretKey(encodedBase64SecretKey);

        assertFullMapping(properties, expected);
    }

    private byte[] generateKey()
            throws NoSuchAlgorithmException
    {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        SecretKey key = generator.generateKey();
        return key.getEncoded();
    }
}
