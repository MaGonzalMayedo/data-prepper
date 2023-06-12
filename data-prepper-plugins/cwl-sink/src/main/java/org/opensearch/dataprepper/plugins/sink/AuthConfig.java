package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class will overlook the json definitions for auth:
 */
public class AuthConfig {
    @JsonProperty("region")
    private String region = "";

    @JsonProperty("role_arn")
    private String role_arn = "";

    public String getRegion() {
        return region;
    }

    public String getRole_arn() {
        return role_arn;
    }
}
