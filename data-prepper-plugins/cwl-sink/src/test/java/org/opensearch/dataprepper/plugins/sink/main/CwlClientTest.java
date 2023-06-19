package org.opensearch.dataprepper.plugins.sink.main;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.CwlClient;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CwlClientTest {
//    private CloudWatchLogsClient mockClient;
//    public Buffer getMockBuffer() {
//        InMemoryBuffer mockBuffer = mock(InMemoryBuffer.class);
//        when(mockBuffer.getEventCount()).thenReturn(10);
//        when(mockBuffer.getEvent()).thenReturn(new Record<Event>()
//
//        return mockBuffer;
//    }
//    public CwlClient getMockClient() {
//        return new CwlClient(getMockBuffer(), "testGroup", "testStream", 10, 10);
//    }
//
//    //Test when
}
