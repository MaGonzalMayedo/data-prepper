package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

/**
 * Class will oversee the json definitions for auth:
 */
public class AuthConfig {
    @JsonProperty("region")
    @NotEmpty
    private String region = "";

    @JsonProperty("role_arn")
    @NotEmpty
    private String role_arn = "";

    public String getRegion() {
        return region;
    }

    public String getRole_arn() {
        return role_arn;
    }
}
