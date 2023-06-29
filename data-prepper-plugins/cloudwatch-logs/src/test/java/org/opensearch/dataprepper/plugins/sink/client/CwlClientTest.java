package org.opensearch.dataprepper.plugins.sink.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBuffer;
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
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

//TODO: Add Codec session.
//TODO: Finish adding feature for ARN reading.

public class CwlClientTest {
    private CloudWatchLogsClient mockClient;
    private CwlSinkConfig cwlSinkConfig;
    private ThresholdConfig thresholdConfig;
    private AwsConfig awsConfig;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private BufferFactory bufferFactory;
    private Buffer buffer;
    private final String TEST_LOG_GROUP = "TESTGROUP";
    private final String TEST_LOG_STREAM = "TESTSTREAM";
    private final int DEFAULT_BATCH_SIZE = 10;
    private final int DEFAULT_RETRY_COUNT = 10;
    private final String DEFAULT_REGION = "us-east-1";
    private final String DEFAULT_ARN = "test:urn";
    @BeforeEach
    void setUp() {
        cwlSinkConfig = mock(CwlSinkConfig.class);
        thresholdConfig = new ThresholdConfig(); //Class can stay as is.
        awsConfig = mock(AwsConfig.class);
        bufferFactory = mock(InMemoryBufferFactory.class);
        buffer = mock(InMemoryBuffer.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

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
    }

    CwlClient getClientWithMemoryBuffer() {
        CwlClient cwlSinkClient = new CwlClient(buffer, clientConfig.getLogGroup(), clientConfig.getLogStream(),
                 clientConfig.getRetryCount(), );
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
        buffer = new InMemoryBufferFactory().getBuffer();
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