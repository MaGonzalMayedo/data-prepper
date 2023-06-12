package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class CwlSinkConfig {

    @JsonProperty("path")
    @NotEmpty
    private String path = "src/resources/file-test-sample-output.txt";

    @JsonProperty("client_config")
    @NotEmpty
    private ClientConfig clientConfig;

    @JsonProperty("auth_config")
    @NotEmpty
    private AuthConfig authConfig;

    public String getPath() {
        return path;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public AuthConfig getAuthConfig() {
        return authConfig;
    }
}
