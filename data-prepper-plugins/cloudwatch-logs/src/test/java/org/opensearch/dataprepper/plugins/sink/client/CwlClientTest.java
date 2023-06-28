package org.opensearch.dataprepper.plugins.sink.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CwlSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

//TODO: Add Codec session.
//TODO: Finish adding feature for ARN reading.

public class CwlClientTest {
    private CloudWatchLogsClient mockClient;
    private CwlSinkConfig cwlSinkConfig;
    private ThresholdConfig thresholdConfig;
    private AwsConfig awsConfig;
    private Buffer testBuffer;
    private final String TEST_LOG_GROUP = "TESTGROUP";
    private final String TEST_LOG_STREAM = "TESTSTREAM";
    private final int DEFAULT_BATCH_SIZE = 10;
    private final int DEFAULT_RETRY_COUNT = 10;

    private final String DEFAULT_REGION = "us-east-1";
    private final String DEFAULT_ARN = "test:urn";
    @BeforeEach
    void setUp() {
        cwlSinkConfig = mock(CwlSinkConfig.class);
        thresholdConfig = new ThresholdConfig();
        awsConfig = mock(AwsConfig.class);

        when(cwlSinkConfig.getLogGroup()).thenReturn(TEST_LOG_GROUP);
        when(cwlSinkConfig.getLogStream()).thenReturn(TEST_LOG_STREAM);
        when(cwlSinkConfig.getBufferType()).thenReturn("in_memory");
        when(cwlSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(cwlSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);

        when(awsConfig.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when()
    }

    CwlClient getClientWithMemoryBuffer() {
        CwlClient cwlSinkClient = new CwlClient(testBuffer, clientConfig.getLogGroup(), clientConfig.getLogStream(),
                clientConfig.getBatchSize(), clientConfig.getRetryCount());
        cwlSinkClient.setCloudWatchLogsClient(mockClient);

        return cwlSinkClient;
    }

    void setMockClientNoErrors() {
        mockClient = mock(CloudWatchLogsClient.class);
        doNothing().when(mockClient).putLogEvents(any(PutLogEventsRequest.class));
    }

    void setMockClientThrowCWLException() {
        mockClient = mock(CloudWatchLogsClient.class);
        doThrow(CloudWatchLogsException.class).when(mockClient).putLogEvents(any(PutLogEventsRequest.class));
    }

    void setBufferWithData() {
        testBuffer = new InMemoryBufferFactory().getBuffer();
    }

    Collection<Record<Event>> getSampleRecords() {
        ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < DEFAULT_BATCH_SIZE; i++) {
            returnCollection.add(new Record<>(JacksonEvent.fromMessage("testMessage")));
        }
        return returnCollection;
    }

    @Test
    void client_creation_test() {
        CwlClient cwlClient = getClientWithMemoryBuffer();
    }

    @Test
    void check_empty_buffer_test() {
        doNothing();
    }

    @Test
    void retry_count_limit_reached_test() {
        setBufferWithData();
        setMockClientThrowCWLException();
        CwlClient cwlClient = getClientWithMemoryBuffer();

        assertThrows(RuntimeException.class, () -> cwlClient.output(getSampleRecords()));
    }

    @Test
    void successful_transmission_test() {
        setBufferWithData();
        setMockClientNoErrors();
        CwlClient cwlClient = getClientWithMemoryBuffer();
    }
}