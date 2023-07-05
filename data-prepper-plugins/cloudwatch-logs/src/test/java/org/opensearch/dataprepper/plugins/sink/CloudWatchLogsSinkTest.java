package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CloudWatchLogsSinkTest {
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    private AwsConfig awsConfig;
    private ThresholdConfig thresholdConfig;
    private PluginSetting pluginSetting;
    private PluginMetrics pluginMetrics;
    private AwsCredentialsSupplier awsCredentialsSupplier;

    public static final String PIPELINE_NAME = "test_pipeline";
    public static final String PLUGIN_NAME = "cwl-sink";

    @BeforeEach
    void setUp() {
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);
        awsConfig = mock(AwsConfig.class);
        thresholdConfig = new ThresholdConfig();
        pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

        when(cloudWatchLogsSinkConfig.getBufferType()).thenReturn(CloudWatchLogsSinkConfig.DEFAULT_BUFFER_TYPE);
        when(cloudWatchLogsSinkConfig.getLogGroup()).thenReturn("testGroup");
        when(cloudWatchLogsSinkConfig.getLogStream()).thenReturn("testStream");
        when(cloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(cloudWatchLogsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);

        when(pluginSetting.getName()).thenReturn(PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PIPELINE_NAME);
    }

    CloudWatchLogsSink getTestableSink() {
        return new CloudWatchLogsSink(pluginSetting, pluginMetrics, cloudWatchLogsSinkConfig, awsCredentialsSupplier);
    }

    @Test
    void check_if_sink_not_null_test() {
        CloudWatchLogsSink cloudWatchLogsSink = getTestableSink();

        assertNotNull(cloudWatchLogsSink);
    }

    @Test
    void check_is_not_ready() {
        CloudWatchLogsSink cloudWatchLogsSink = getTestableSink();

        assertThat(cloudWatchLogsSink.isReady(), equalTo(false));
    }

    @Test
    void check_is_ready_test() {
        CloudWatchLogsSink cloudWatchLogsSink = getTestableSink();

        cloudWatchLogsSink.doInitialize();
        assertThat(cloudWatchLogsSink.isReady(), equalTo(true));
    }


}
