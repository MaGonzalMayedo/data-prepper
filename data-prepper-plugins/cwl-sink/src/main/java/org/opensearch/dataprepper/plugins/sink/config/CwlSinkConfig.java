package org.opensearch.dataprepper.plugins.sink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;

public class CwlSinkConfig {
    public static final String DEFAULT_BUFFER_TYPE = "in_memory";

    //Class was utilized from the
    @JsonProperty("aws_config")
    private AwsAuthenticationOptions awsConfig;

    @JsonProperty("threshold_config")
    private ThresholdConfig thresholdConfig;

    @JsonProperty("buffer_type")
    private String bufferType = DEFAULT_BUFFER_TYPE;

    @JsonProperty("log_group")
    @NotEmpty
    private String logGroup = null;

    @JsonProperty("log_stream")
    @NotEmpty
    private String logStream = null;

    public AwsAuthenticationOptions getAwsConfig() {
        return awsConfig;
    }

    public ThresholdConfig getThresholdConfig() {
        return thresholdConfig;
    }

    public String getBufferType() {
        return bufferType;
    }

    public String getLogGroup() {
        return logGroup;
    }

    public String getLogStream() {
        return logStream;
    }
}
