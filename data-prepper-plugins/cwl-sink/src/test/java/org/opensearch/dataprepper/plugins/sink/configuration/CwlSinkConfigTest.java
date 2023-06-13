package org.opensearch.dataprepper.plugins.sink.configuration;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.CwlSink;
import org.opensearch.dataprepper.plugins.sink.CwlSinkConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CwlSinkConfigTest {
    @Test
    void check_null_auth_config_test() {
        assertThat(new CwlSinkConfig().getAuthConfig(), equalTo(null));
    }

    @Test
    void check_null_client_config_test() {
        assertThat(new CwlSinkConfig().getClientConfig(), equalTo(null));
    }
}
