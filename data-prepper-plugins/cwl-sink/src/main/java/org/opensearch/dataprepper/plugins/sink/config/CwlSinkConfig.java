package org.opensearch.dataprepper.plugins.sink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.ClientConfig;

public class CwlSinkConfig {
    public static final String DEFAULT_BUFFER_TYPE = "in_memory";

    @JsonProperty("client_config")
    private ClientConfig clientConfig;

    @JsonProperty("aws_config")
    private AwsConfig awsConfig;

    @JsonProperty("threshold_config")
    private ThresholdConfig thresholdConfig;

    @JsonProperty("buffer_type")
    private String bufferType = DEFAULT_BUFFER_TYPE;

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public ThresholdConfig getThresholdConfig() {
        return thresholdConfig;
    }

    public String getBufferType() {
        return bufferType;
    }
}
