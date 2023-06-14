package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

public class CwlSinkConfig {

    //TODO: Remove this after the CWL SDK has been implemented in the main class.
    @JsonProperty("client_config")
    private ClientConfig clientConfig;

    @JsonProperty("auth_config")
    private AuthConfig authConfig;
    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public AuthConfig getAuthConfig() {
        return authConfig;
    }
}
