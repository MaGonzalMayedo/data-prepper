package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CwlSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CwlSinkTest {
    private CwlSinkConfig cwlSinkConfig;
    private AwsConfig awsConfig;
    private ThresholdConfig thresholdConfig;
    private PluginSetting pluginSetting;
    private PluginMetrics pluginMetrics;
    private AwsCredentialsSupplier awsCredentialsSupplier;

    public static final String PIPELINE_NAME = "test_pipeline";
    public static final String PLUGIN_NAME = "cwl-sink";

    @BeforeEach
    void setUp() {
        cwlSinkConfig = mock(CwlSinkConfig.class);
        awsConfig = mock(AwsConfig.class);
        thresholdConfig = new ThresholdConfig();
        pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

        when(cwlSinkConfig.getBufferType()).thenReturn(CwlSinkConfig.DEFAULT_BUFFER_TYPE);
        when(cwlSinkConfig.getLogGroup()).thenReturn("testGroup");
        when(cwlSinkConfig.getLogStream()).thenReturn("testStream");
        when(cwlSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(cwlSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);

        when(pluginSetting.getName()).thenReturn(PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PIPELINE_NAME);
    }

    CwlSink getTestableSink() {
        return new CwlSink(pluginSetting, pluginMetrics, cwlSinkConfig, awsCredentialsSupplier);
    }

    @Test
    void check_if_sink_not_null_test() {
        CwlSink cwlSink = getTestableSink();

        assertNotNull(cwlSink);
    }

    @Test
    void check_is_not_ready() {
        CwlSink cwlSink = getTestableSink();

        assertThat(cwlSink.isReady(), equalTo(false));
    }

    @Test
    void check_is_ready_test() {
        CwlSink cwlSink = getTestableSink();

        cwlSink.doInitialize();
        assertThat(cwlSink.isReady(), equalTo(true));
    }


}
