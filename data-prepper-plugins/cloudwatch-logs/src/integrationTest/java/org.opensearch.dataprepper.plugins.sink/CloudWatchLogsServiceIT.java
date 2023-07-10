/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsClientFactory;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsService;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.threshold.ThresholdCheck;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.Thread.sleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

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
    private CloudWatchLogsClient cloudWatchLogsClient;
    public static final String LOG_GROUP = "testLogGroup"; //TODO: Change these to system properties once plugin is added.
    public static final String LOG_STREAM = "testLogStream"; //TODO: Change these to system properties once the plugin is added.
    public static final String PIPELINE_NAME = "test_pipeline";
    public static final String PLUGIN_NAME = "cwl-sink";
    public static final String TEST_LOG_MESSAGE = "testing CloudWatchLogs Plugin";
    private Counter requestSuccessCounter;
    private Counter requestFailCounter;
    private Counter successEventCounter;
    private Counter failedEventCounter;

    @BeforeEach
    void setUp() {
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);
        awsConfig = mock(AwsConfig.class);
        thresholdConfig = new ThresholdConfig();
        thresholdCheck = new ThresholdCheck(1, thresholdConfig.getMaxEventSize() * 1024,
                thresholdConfig.getMaxRequestSize(), thresholdConfig.getLogSendInterval());
        pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

        requestSuccessCounter = mock(Counter.class);
        requestFailCounter = mock(Counter.class);
        successEventCounter = mock(Counter.class);
        failedEventCounter = mock(Counter.class);

        bufferFactory = new InMemoryBufferFactory();
        buffer = bufferFactory.getBuffer();

        when(cloudWatchLogsSinkConfig.getBufferType()).thenReturn(CloudWatchLogsSinkConfig.DEFAULT_BUFFER_TYPE);
        when(cloudWatchLogsSinkConfig.getLogGroup()).thenReturn(LOG_GROUP);
        when(cloudWatchLogsSinkConfig.getLogStream()).thenReturn(LOG_STREAM);
        when(cloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(cloudWatchLogsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);

        when(pluginMetrics.counter(CloudWatchLogsService.NUMBER_OF_RECORDS_PUSHED_TO_CWL_SUCCESS)).thenReturn(successEventCounter);
        when(pluginMetrics.counter(CloudWatchLogsService.REQUESTS_SUCCEEDED)).thenReturn(requestSuccessCounter);
        when(pluginMetrics.counter(CloudWatchLogsService.NUMBER_OF_RECORDS_PUSHED_TO_CWL_FAIL)).thenReturn(failedEventCounter);
        when(pluginMetrics.counter(CloudWatchLogsService.REQUESTS_FAILED)).thenReturn(requestFailCounter);

        when(awsConfig.getAwsRegion()).thenReturn(Region.US_EAST_1);

        cloudWatchLogsClient = CloudWatchLogsClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier);

        when(pluginSetting.getName()).thenReturn(PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PIPELINE_NAME);
    }

    CloudWatchLogsService getTestableService() {
        return new CloudWatchLogsService(cloudWatchLogsClient, cloudWatchLogsSinkConfig, buffer,
                pluginMetrics, thresholdCheck, thresholdConfig.getRetryCount(),
                thresholdConfig.getBackOffTime());
    }

    ArrayList<Record<Event>> getSampleEvent() {
        final ArrayList<Record<Event>> eventList = new ArrayList<>();

        Record<Event> testEvent = new Record<>(JacksonEvent.fromMessage(TEST_LOG_MESSAGE));
        eventList.add(testEvent);

        return eventList;
    }

    Collection<Record<Event>> getSampleRecordsLarge(int numberOfRecords, int sizeOfRecordsBytes) {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        final String testMessage = "a";
        for (int i = 0; i < numberOfRecords; i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage(testMessage.repeat(sizeOfRecordsBytes));
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    String retrieveLatestLogEvent() {
        GetLogEventsRequest getLogEventsRequest = GetLogEventsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamName(LOG_STREAM)
                .startFromHead(false)
                .build();

        GetLogEventsResponse getLogEventsResponse = cloudWatchLogsClient.getLogEvents(getLogEventsRequest);
        List<OutputLogEvent> logEvents = getLogEventsResponse.events();

        return logEvents.get(0).message();
    }

    int retrieveLogListSize() {
        GetLogEventsRequest getLogEventsRequest = GetLogEventsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamName(LOG_STREAM)
                .startFromHead(false)
                .build();

        return cloudWatchLogsClient.getLogEvents(getLogEventsRequest).events().size();
    }

    /**
     * Want to send a log and retrieve it and compare body messages to ensure the log
     * was properly ingested.
     */
    @Test
    void check_logs_successfully_ingested() throws InterruptedException {
        cloudWatchLogsService = getTestableService();
        cloudWatchLogsService.output(getSampleEvent());

        sleep(1000);

        String resultMessage = retrieveLatestLogEvent();
        String compareTo = JacksonEvent.fromMessage(TEST_LOG_MESSAGE).toJsonString();

        assertThat(resultMessage, equalTo(compareTo));
    }

    @Test
    void check_log_count_after_ingestion() throws InterruptedException {
        cloudWatchLogsService = getTestableService();

        int numberOfLogsPrior = retrieveLogListSize();

        cloudWatchLogsService.output(getSampleEvent());

        sleep(1000); //Need to wait for AWS services to sync up after event is received.

        int numberOfLogsAfter = retrieveLogListSize();

        assertThat(numberOfLogsAfter, greaterThan((numberOfLogsPrior)));
    }
}
