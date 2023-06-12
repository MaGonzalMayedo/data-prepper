package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class ClientConfig {
    @JsonProperty("log_group")
    @NotEmpty
    private String logGroup;

    @JsonProperty("log_stream")
    @NotEmpty
    private String logStream;

    @JsonProperty("batch_size")
    @NotEmpty
    private int batchSize;

    @JsonProperty("retry_count")
    @NotEmpty
    private int retryCount;

    private String getLogGroup() {
        return logGroup;
    }

    private String getLogStream() {
        return logStream;
    }

    private int getBatchSize() {
        return batchSize;
    }

    private int getRetryCount() {
        return retryCount;
    }
}