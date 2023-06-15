package org.opensearch.dataprepper.plugins.sink.main;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.AuthConfig;
import org.opensearch.dataprepper.plugins.sink.ClientConfig;
import org.opensearch.dataprepper.plugins.sink.CwlSink;
import org.opensearch.dataprepper.plugins.sink.CwlSinkConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CwlSinkTest {
    private AuthConfig authConfig;
    private ClientConfig clientConfig;
    private CwlSinkConfig cwlSinkConfig;

    private final String TEST_LOG_GROUP = "TESTGROUP";
    private final String TEST_LOG_STREAM = "TESTSTREAM";
    private final int DEFAULT_BATCH_SIZE = 10;
    private final int DEFAULT_RETRY_COUNT = 10;

    CwlSink getTestableClass() {
        return new CwlSink(cwlSinkConfig);
    }

    @BeforeEach
    void setUp() {
        authConfig = new AuthConfig();
        clientConfig = mock(ClientConfig.class);
        cwlSinkConfig = mock(CwlSinkConfig.class);
        when(clientConfig.getLogGroup()).thenReturn(TEST_LOG_GROUP);
        when(clientConfig.getLogStream()).thenReturn(TEST_LOG_STREAM);
        when(clientConfig.getBatchSize()).thenReturn(DEFAULT_BATCH_SIZE);
        when(clientConfig.getRetryCount()).thenReturn(DEFAULT_RETRY_COUNT);
        when(cwlSinkConfig.getAuthConfig()).thenReturn(authConfig);
        when(cwlSinkConfig.getClientConfig()).thenReturn(clientConfig);
    }

    @Test
    void check_initialized_test() {
        CwlSink cwlSink = getTestableClass();
        cwlSink.initialize();
        assertThat(cwlSink.isReady(), equalTo(true));
    }

}
