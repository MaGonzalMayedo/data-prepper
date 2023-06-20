package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CwlSinkConfig {

    //TODO: Remove this after the CWL SDK has been implemented in the main class.
    @JsonProperty("client_config")
    private ClientConfig clientConfig;

    @JsonProperty("auth_config")
    private AwsConfig awsConfig;
    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public AwsConfig getAuthConfig() {
        return awsConfig;
    }
}
