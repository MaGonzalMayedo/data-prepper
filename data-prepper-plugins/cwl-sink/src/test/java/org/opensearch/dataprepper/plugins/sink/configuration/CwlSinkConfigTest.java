package org.opensearch.dataprepper.plugins.sink.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.CwlSink;
import org.opensearch.dataprepper.plugins.sink.CwlSinkConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CwlSinkConfigTest {
    //Client Config:
    public static final String LOG_GROUP = "testGroup";
    public static final String LOG_STREAM = "testStream";
    public static final String BUFFER_TYPE = "in_memory";
    public static final int BATCH_SIZE = 10;
    public static final int MAX_RETRIES = 10;
    //Auth Config:
    public static final String REGION = "us-east-1";

    @BeforeEach
    void setUp() {

    }

    @Test
    void check_null_auth_config_test() {
        assertThat(new CwlSinkConfig().getAuthConfig(), equalTo(null));
    }

    @Test
    void check_null_client_config_test() {
        assertThat(new CwlSinkConfig().getClientConfig(), equalTo(null));
    }
}
