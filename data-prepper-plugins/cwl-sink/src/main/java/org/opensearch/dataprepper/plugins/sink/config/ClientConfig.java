package org.opensearch.dataprepper.plugins.sink.config;

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

    public String getLogGroup() {
        return logGroup;
    }

    public String getLogStream() {
        return logStream;
    }
}