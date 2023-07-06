/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsService;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.threshold.ThresholdCheck;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CloudWatchLogsServiceIT {
    @Mock
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    @Mock
    private AwsConfig awsConfig;
    @Mock
    private ThresholdConfig thresholdConfig;
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private BufferFactory bufferFactory;
    @Mock
    private Buffer buffer;
    @Mock
    private ThresholdCheck thresholdCheck;
    @Mock
    private CloudWatchLogsService cloudWatchLogsService;
    public static final String LOG_GROUP = "testLogGroup";
    public static final String LOG_STREAM = "testLogStream";
    public static final String PIPELINE_NAME = "test_pipeline";
    public static final String PLUGIN_NAME = "cwl-sink";
    private Counter requestSuccessCounter;
    private Counter requestFailCounter;
    private Counter successEventCounter;
    private Counter failedEventCounter;

    @BeforeEach
    void setUp() {
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);
        awsConfig = mock(AwsConfig.class);
        thresholdConfig = new ThresholdConfig();
        pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

        requestSuccessCounter = mock(Counter.class);
        requestFailCounter = mock(Counter.class);
        successEventCounter = mock(Counter.class);
        failedEventCounter = mock(Counter.class);

        when(cloudWatchLogsSinkConfig.getBufferType()).thenReturn(CloudWatchLogsSinkConfig.DEFAULT_BUFFER_TYPE);
        when(cloudWatchLogsSinkConfig.getLogGroup()).thenReturn(LOG_GROUP);
        when(cloudWatchLogsSinkConfig.getLogStream()).thenReturn(LOG_STREAM);
        when(cloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(cloudWatchLogsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);

        when(pluginSetting.getName()).thenReturn(PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PIPELINE_NAME);
    }

    CloudWatchLogsSink getTestableSink() {
        return new CloudWatchLogsSink(pluginSetting, pluginMetrics, cloudWatchLogsSinkConfig, awsCredentialsSupplier);
    }
}
