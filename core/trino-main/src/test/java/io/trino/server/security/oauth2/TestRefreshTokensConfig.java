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

import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestRefreshTokensConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RefreshTokensConfig.class)
                .setRefreshTokenExpiration(succinctDuration(10, MINUTES))
                .setAccessTokenExpiration(succinctDuration(1, HOURS))
                .setIssuer("Trino_coordinator")
                .setAudience("Trino_coordinator"));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.oauth2.refresh-tokens.refresh.timeout", "24h")
                .put("http-server.authentication.oauth2.refresh-tokens.access-token.expiration", "30m")
                .put("http-server.authentication.oauth2.refresh-tokens.access-token.issuer", "issuer")
                .put("http-server.authentication.oauth2.refresh-tokens.access-token.audience", "audience")
                .buildOrThrow();

        RefreshTokensConfig expected = new RefreshTokensConfig()
                .setRefreshTokenExpiration(succinctDuration(24, HOURS))
                .setAccessTokenExpiration(succinctDuration(30, MINUTES))
                .setIssuer("issuer")
                .setAudience("audience");

        assertFullMapping(properties, expected);
    }
}
