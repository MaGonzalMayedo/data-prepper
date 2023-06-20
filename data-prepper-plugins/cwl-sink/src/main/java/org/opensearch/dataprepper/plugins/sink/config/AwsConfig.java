package org.opensearch.dataprepper.plugins.sink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

/**
 * Class will oversee the json definitions for auth:
 */
public class AwsConfig {

    @JsonProperty("region")
    private String region = null;

    @JsonProperty("sts_role_arn")
    private String role_arn = null;

    @JsonProperty("path_to_credentials")
    private String pathToCredentials = null;

    public String getRegion() {
        return region;
    }

    public String getRole_arn() {
        return role_arn;
    }

    public String getPathToCredentials() {
        return pathToCredentials;
    }
}
