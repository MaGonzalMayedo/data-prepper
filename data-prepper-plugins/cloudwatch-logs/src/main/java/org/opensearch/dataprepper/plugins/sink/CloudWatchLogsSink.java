package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsService;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsClientFactory;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.threshold.ThresholdCheck;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import java.util.Collection;

/**
 * TODO: Can add PluginFactory once we add the DLQ as we need to load in the S3 plugin feature.
 */

/**
 * The CwlSink class is in charge of staging log events before pushing them
 * to AWS CloudWatchLogs Services.
 */
@DataPrepperPlugin(name = "cwl-sink", pluginType = Sink.class, pluginConfigurationType = CloudWatchLogsSinkConfig.class)
public class CloudWatchLogsSink extends AbstractSink<Record<Event>> {
    private final AwsConfig awsConfig;
    private final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    private final ThresholdConfig thresholdConfig;
    private final PluginMetrics pluginMetrics;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private CloudWatchLogsClient cloudWatchLogsClient;
    private BufferFactory bufferFactory;
    private Buffer buffer;
    private ThresholdCheck thresholdCheck;
    private CloudWatchLogsService cloudWatchLogsService;
    private boolean isStopRequested;
    private boolean isInitialized;
    @DataPrepperPluginConstructor
    public CloudWatchLogsSink(final PluginSetting pluginSetting,
                              final PluginMetrics pluginMetrics,
                              final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig,
                              final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);

        this.pluginMetrics = pluginMetrics;
        this.cloudWatchLogsSinkConfig = cloudWatchLogsSinkConfig;
        this.awsConfig = cloudWatchLogsSinkConfig.getAwsConfig();
        this.thresholdConfig = cloudWatchLogsSinkConfig.getThresholdConfig();
        this.awsCredentialsSupplier = awsCredentialsSupplier;

        if (cloudWatchLogsSinkConfig.getBufferType().equals("in_memory")) {
            bufferFactory = new InMemoryBufferFactory();
        }

        buffer = bufferFactory.getBuffer();
        cloudWatchLogsClient = CloudWatchLogsClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier);

        //Applies the conversion to kilobytes:
        thresholdCheck = new ThresholdCheck(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSize() * 1000,
                thresholdConfig.getMaxRequestSize(),thresholdConfig.getLogSendInterval());
    }

    @Override
    public void doInitialize() {
        cloudWatchLogsService = new CloudWatchLogsService(cloudWatchLogsClient, cloudWatchLogsSinkConfig, buffer,
                pluginMetrics, thresholdCheck, thresholdConfig.getRetryCount(), thresholdConfig.getBackOffTime());
        isInitialized = true;
    }

    @Override
    public void doOutput(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }

        cloudWatchLogsService.output(records);
    }

    @Override
    public boolean isReady() {
        return isInitialized;
    }
}
