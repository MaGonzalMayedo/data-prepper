package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.introspect.TypeResolutionContext;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Null;

/**
 * Class will oversee the configuration of the CWL SDK:
 */
public class ClientConfig {
    @JsonProperty("log_group")
    @NotEmpty
    private String logGroup = null;

    @JsonProperty("log_stream")
    @NotEmpty
    private String logStream = null;

    @JsonProperty("buffer_type")
    private String bufferType = "in_memory";

    @JsonProperty("batch_size")
    private int batchSize = 10;

    @JsonProperty("retry_count")
    private int retryCount = 10;

    public String getLogGroup() {
        return logGroup;
    }

    public String getLogStream() {
        return logStream;
    }

    public String getBufferType() {
        return bufferType;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getRetryCount() {
        return retryCount;
    }
}