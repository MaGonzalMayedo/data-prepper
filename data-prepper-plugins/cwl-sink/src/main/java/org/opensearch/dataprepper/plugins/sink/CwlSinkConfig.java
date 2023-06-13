package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

public class CwlSinkConfig {

    //TODO: Remove this after the CWL SDK has been implemented in the main class.
    @JsonProperty("path")
    @NotEmpty
    private String path = "/Users/alemayed/Amazon_Projects/test_outputs/test_output.txt";

    @JsonProperty("client_config")
    @Valid
    private ClientConfig clientConfig;

    @JsonProperty("auth_config")
    @Valid
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
