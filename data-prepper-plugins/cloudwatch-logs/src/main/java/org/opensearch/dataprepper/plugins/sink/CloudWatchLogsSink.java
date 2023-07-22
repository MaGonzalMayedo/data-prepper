/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

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
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsDispatcher;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsService;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsClientFactory;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.metrics.CloudWatchLogsMetrics;
import org.opensearch.dataprepper.plugins.sink.packaging.ThreadTaskEvents;
import org.opensearch.dataprepper.plugins.sink.push_condition.CloudWatchLogsLimits;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * TODO: Can add PluginFactory once we add the DLQ as we need to load in the S3 plugin feature.
 */

/**
 * The CwlSink class is in charge of staging log events before pushing them
 * to AWS CloudWatchLogs Services.
 */
@DataPrepperPlugin(name = "cloudwatchlogs-sink", pluginType = Sink.class, pluginConfigurationType = CloudWatchLogsSinkConfig.class)
public class CloudWatchLogsSink extends AbstractSink<Record<Event>> {
    public static final int BLOCKING_QUEUE_SIZE = 100;
    private final AwsConfig awsConfig;
    private final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    private final ThresholdConfig thresholdConfig;
    private final PluginMetrics pluginMetrics;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private CloudWatchLogsClient cloudWatchLogsClient;
    private BufferFactory bufferFactory;
    private Buffer buffer;
    private final CloudWatchLogsLimits cloudWatchLogsLimits;
    private final CloudWatchLogsService cloudWatchLogsService;
    private final CloudWatchLogsDispatcher cloudWatchLogsDispatcher;
    private final CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private final BlockingQueue<ThreadTaskEvents> taskQueue;
    private boolean isInitialized;
    @DataPrepperPluginConstructor
    public CloudWatchLogsSink(final PluginSetting pluginSetting,
                              final PluginMetrics pluginMetrics,
                              final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig,
                              final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);

        this.pluginMetrics = pluginMetrics;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.cloudWatchLogsSinkConfig = cloudWatchLogsSinkConfig;
        this.awsConfig = cloudWatchLogsSinkConfig.getAwsConfig();
        this.thresholdConfig = cloudWatchLogsSinkConfig.getThresholdConfig();


        if (cloudWatchLogsSinkConfig.getBufferType().equals("in_memory")) {
            bufferFactory = new InMemoryBufferFactory();
        }
        buffer = bufferFactory.getBuffer();

        taskQueue = new ArrayBlockingQueue<>(BLOCKING_QUEUE_SIZE);

        cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSize(),thresholdConfig.getLogSendInterval());
        cloudWatchLogsMetrics = new CloudWatchLogsMetrics(pluginMetrics);
        cloudWatchLogsClient = CloudWatchLogsClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier);
        cloudWatchLogsDispatcher = new CloudWatchLogsDispatcher(taskQueue, cloudWatchLogsClient, cloudWatchLogsMetrics,
                cloudWatchLogsSinkConfig.getLogGroup(), cloudWatchLogsSinkConfig.getLogStream(),
                thresholdConfig.getRetryCount(), thresholdConfig.getBackOffTime());

        cloudWatchLogsService = new CloudWatchLogsService(buffer, cloudWatchLogsLimits, cloudWatchLogsDispatcher, taskQueue);
    }

    @Override
    public void doInitialize() {
        isInitialized = true;
    }

    @Override
    public void doOutput(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }

        cloudWatchLogsService.processLogEvents(records);
    }

    @Override
    public boolean isReady() {
        return isInitialized;
    }
}