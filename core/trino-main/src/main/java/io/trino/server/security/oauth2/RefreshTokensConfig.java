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
import io.airlift.units.Duration;

import javax.validation.constraints.NotEmpty;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class RefreshTokensConfig
{
    private Duration refreshTokenExpiration = Duration.succinctDuration(10.00, MINUTES);
    private Duration accessTokenExpiration = Duration.succinctDuration(1.00, HOURS);
    private String issuer = "Trino_coordinator";
    private String audience = "Trino_coordinator";

    public Duration getRefreshTokenExpiration()
    {
        return refreshTokenExpiration;
    }

    @Config("http-server.authentication.oauth2.refresh-tokens.refresh.timeout")
    @ConfigDescription("Time during which expiration will happen. It triggers only after the accessToken has expired and is always rounded to seconds")
    public RefreshTokensConfig setRefreshTokenExpiration(Duration refreshTokenExpiration)
    {
        this.refreshTokenExpiration = refreshTokenExpiration;
        return this;
    }

    public Duration getAccessTokenExpiration()
    {
        return accessTokenExpiration;
    }

    @Config("http-server.authentication.oauth2.refresh-tokens.access-token.expiration")
    @ConfigDescription("Time during which accessToken will be used to identify a user. It needs to be equal or lower than duration of actual accessToken issued by IdP")
    public RefreshTokensConfig setAccessTokenExpiration(Duration accessTokenExpiration)
    {
        this.accessTokenExpiration = accessTokenExpiration;
        return this;
    }

    @NotEmpty
    public String getIssuer()
    {
        return issuer;
    }

    @Config("http-server.authentication.oauth2.refresh-tokens.access-token.issuer")
    @ConfigDescription("Issuer representing this coordinator instance, that will be used in issued token. In addition current Version will be added to it")
    public RefreshTokensConfig setIssuer(String issuer)
    {
        this.issuer = issuer;
        return this;
    }

    @NotEmpty
    public String getAudience()
    {
        return audience;
    }

    @Config("http-server.authentication.oauth2.refresh-tokens.access-token.audience")
    @ConfigDescription("Audience representing this coordinator instance, that will be used in issued token")
    public RefreshTokensConfig setAudience(String audience)
    {
        this.audience = audience;
        return this;
    }
}
