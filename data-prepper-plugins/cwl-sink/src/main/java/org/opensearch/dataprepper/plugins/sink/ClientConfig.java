package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

/**
 * Class will oversee the configuration of the CWL SDK:
 */
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

    public String getLogGroup() {
        return logGroup;
    }

    public String getLogStream() {
        return logStream;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getRetryCount() {
        return retryCount;
    }
}