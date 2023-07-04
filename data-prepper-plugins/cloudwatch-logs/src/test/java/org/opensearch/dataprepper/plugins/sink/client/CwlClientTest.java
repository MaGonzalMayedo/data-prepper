package org.opensearch.dataprepper.plugins.sink.client;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CwlSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.exception.RetransmissionLimitException;
import org.opensearch.dataprepper.plugins.sink.threshold.ThresholdCheck;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

//TODO: Add Codec session.
//TODO: Finish adding feature for ARN reading.

public class CwlClientTest {
    private CloudWatchLogsClient mockClient;
    private PutLogEventsResponse putLogEventsResponse;
    private CwlSinkConfig cwlSinkConfig;
    private ThresholdConfig thresholdConfig;
    private ThresholdCheck thresholdCheck;
    private AwsConfig awsConfig;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private BufferFactory bufferFactory;
    private Buffer buffer;
    private PluginMetrics pluginMetrics;
    private Counter requestSuccessCounter;
    private Counter requestFailCounter;
    private Counter successEventCounter;
    private Counter failedEventCounter;
    private final String TEST_LOG_GROUP = "TESTGROUP";
    private final String TEST_LOG_STREAM = "TESTSTREAM";

    @BeforeEach
    void setUp() {
        cwlSinkConfig = mock(CwlSinkConfig.class);

        thresholdConfig = new ThresholdConfig(); //Class can stay as is.
        thresholdCheck = new ThresholdCheck(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSize(),
                thresholdConfig.getMaxBatchSize(), thresholdConfig.getLogSendInterval());

        awsConfig = mock(AwsConfig.class);
        bufferFactory = new InMemoryBufferFactory();
        buffer = bufferFactory.getBuffer();
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

        pluginMetrics = mock(PluginMetrics.class);
        requestSuccessCounter = mock(Counter.class);
        requestFailCounter = mock(Counter.class);
        successEventCounter = mock(Counter.class);
        failedEventCounter = mock(Counter.class);

        final String stsRoleArn = UUID.randomUUID().toString();
        final String externalId = UUID.randomUUID().toString();
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        when(cwlSinkConfig.getLogGroup()).thenReturn(TEST_LOG_GROUP);
        when(cwlSinkConfig.getLogStream()).thenReturn(TEST_LOG_STREAM);
        when(cwlSinkConfig.getBufferType()).thenReturn("in_memory");
        when(cwlSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(cwlSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);

        when(awsConfig.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsConfig.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsConfig.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);
        when(awsConfig.getAwsStsExternalId()).thenReturn(externalId);

        lenient().when(pluginMetrics.counter(CwlClient.NUMBER_OF_RECORDS_PUSHED_TO_CWL_SUCCESS)).thenReturn(successEventCounter);
        lenient().when(pluginMetrics.counter(CwlClient.REQUESTS_SUCCEEDED)).thenReturn(requestSuccessCounter);
        lenient().when(pluginMetrics.counter(CwlClient.NUMBER_OF_RECORDS_PUSHED_TO_CWL_FAIL)).thenReturn(failedEventCounter);
        lenient().when(pluginMetrics.counter(CwlClient.REQUESTS_FAILED)).thenReturn(requestFailCounter);
    }

    CwlClient getCwlClientWithMemoryBuffer() {
        return new CwlClient(mockClient, cwlSinkConfig, buffer, pluginMetrics,
                thresholdCheck, thresholdConfig.getRetryCount(), ThresholdConfig.DEFAULT_BACKOFF_TIME);
    }

    void setMockClientNoErrors() {
        mockClient = mock(CloudWatchLogsClient.class);
        putLogEventsResponse = mock(PutLogEventsResponse.class);
        when(mockClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(putLogEventsResponse);
        when(putLogEventsResponse.rejectedLogEventsInfo()).thenReturn(null);
    }

    void setMockClientThrowCWLException() {
        mockClient = mock(CloudWatchLogsClient.class);
        doThrow(AwsServiceException.class).when(mockClient).putLogEvents(any(PutLogEventsRequest.class));
    }

    Collection<Record<Event>> getSampleRecords(int numberOfRecords) {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    @Test
    void client_creation_test() {
        CwlClient cwlClient = getCwlClientWithMemoryBuffer();
    }

    @Test
    void retry_count_limit_reached_test() {
        setMockClientThrowCWLException();
        CwlClient cwlClient = getCwlClientWithMemoryBuffer();

        try {
            cwlClient.output(getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE * 2));
        } catch (RetransmissionLimitException e) { //TODO: Create a dedicated RuntimeException for this.
            assertThat(e, notNullValue());
        }
    }

    @Test
    void check_failed_event_transmission_test() {
        setMockClientThrowCWLException();
        CwlClient cwlClient = getCwlClientWithMemoryBuffer();

        try {
            cwlClient.output(getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE));
        } catch (RetransmissionLimitException e) {
            verify(failedEventCounter).increment(ThresholdConfig.DEFAULT_BATCH_SIZE);
        }
    }

    @Test
    void check_successful_event_transmission_test() {
        setMockClientNoErrors();
        CwlClient cwlClient = getCwlClientWithMemoryBuffer();

        cwlClient.output(getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE * 2));

        verify(successEventCounter).increment(anyDouble());
    }

    @Test
    void check_failed_event_test() {
        setMockClientThrowCWLException();
        CwlClient cwlClient = getCwlClientWithMemoryBuffer();

        try {
            cwlClient.output(getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE * 4));
        } catch (RetransmissionLimitException e) {
            verify(requestFailCounter, times(4)).increment();
        }
    }

    @Test
    void check_successful_event_test() {
        setMockClientNoErrors();
        CwlClient cwlClient = getCwlClientWithMemoryBuffer();

        cwlClient.output(getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE * 4));

        verify(requestSuccessCounter, times(4)).increment();
    }

    @Test
    void check_event_handles_successfully_released_test() {
        setMockClientNoErrors();
        CwlClient cwlClient = getCwlClientWithMemoryBuffer();

        final Collection<Record<Event>> sampleEvents = getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE * 2);
        final Collection<EventHandle> sampleEventHandles = sampleEvents.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        cwlClient.output(sampleEvents);

        for (EventHandle sampleEventHandle: sampleEventHandles) {
            verify(sampleEventHandle).release(true);
        }
    }

    @Test
    void check_event_handles_failed_released_test() {
        setMockClientThrowCWLException();
        CwlClient cwlClient = getCwlClientWithMemoryBuffer();

        final Collection<Record<Event>> sampleEvents = getSampleRecords(ThresholdConfig.DEFAULT_BATCH_SIZE);
        final Collection<EventHandle> sampleEventHandles = sampleEvents.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        try {
            cwlClient.output(sampleEvents);
        } catch (RetransmissionLimitException e) {
            for (EventHandle sampleEventHandle: sampleEventHandles) {
                verify(sampleEventHandle).release(false);
            }
        }
    }
}