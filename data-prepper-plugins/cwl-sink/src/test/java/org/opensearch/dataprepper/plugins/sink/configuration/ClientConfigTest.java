package org.opensearch.dataprepper.plugins.sink.configuration;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.config.ClientConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ClientConfigTest {
    @Test
    void check_null_log_group_test() {
        assertThat(new ClientConfig().getLogGroup(), equalTo(null));
    }
    @Test
    void check_null_log_stream_test() {
        assertThat(new ClientConfig().getLogStream(), equalTo(null));
    }

    @Test
    void check_default_buffer_type_test() {
        assertThat(new ClientConfig().getBufferType(), equalTo("in_memory"));
    }

    @Test
    void check_default_batch_size() {
        assertThat(new ClientConfig().getBatchSize(), equalTo(10));
    }

    @Test
    void check_default_retry_count() {
        assertThat(new ClientConfig().getRetryCount(), equalTo(10));
    }
}
