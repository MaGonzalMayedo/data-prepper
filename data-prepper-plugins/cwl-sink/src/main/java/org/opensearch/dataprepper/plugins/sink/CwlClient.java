package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
//import software.amazon.awssdk.services.cloudwatch.endpoints.internal.Value;
import software.amazon.awssdk.services.cloudwatch.endpoints.internal.Value;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClientBuilder;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

//TODO: Might have to encapsulate the buffer writeEvent method in a try and catch statement.
//TODO: Look over EventHandle.
//TODO: Need to add logic for max amounts of events that can be buffered.

/**
 * CwlClient is an interface class that simplifies method calls to post logs to
 * CloudWatch logs. It receives a collection of events and interprets them into
 * a message to send to CWL.
 * TODO: Need to add Client Configuration class. (ARN roles and region bounds)
 */
public class CwlClient {
    private static final Logger LOG = LoggerFactory.getLogger(CwlClient.class);
    private final Buffer buffer;
    private final String logGroup;
    private final String logStream;
    private final int batchSize;
    private final int maxLogsQueued = 0; //TODO: Make use of this parameter if needed.
    private final int retryCount;
    private CloudWatchLogsClient cloudWatchLogsClient;
    private int failCount = 0; //Counter to be used on the fly during error handling.
    private boolean failedPost;

    public CwlClient(final Buffer buffer, String logGroup, final String logStream, final int batchSize, final int retryCount) {
        this.buffer = buffer;
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.batchSize = batchSize;
        this.retryCount = retryCount;
        cloudWatchLogsClient = CloudWatchLogsClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CWL.
     * Implements simple batch limit buffer. (Sends once batch size is reached)
     * @param logs Collection of Record events which hold log data.
     */
    public void output(final Collection<Record<Event>> logs) {
        for (Record<Event> singleLog: logs) {
            buffer.writeEvent(singleLog);
            if (buffer.getEventCount() >= batchSize) {
                LOG.info("Attempting to push logs!");
                pushLogs();
            }
        }
    }

    private void pushLogs() {
        ArrayList<InputLogEvent> logEventList = new ArrayList<>();
        failedPost = true;

        while (buffer.getEventCount() > 0) {
            InputLogEvent tempLogEvent = InputLogEvent.builder()
                    .message(buffer.getEvent().getData().toJsonString())
                    .timestamp(System.currentTimeMillis())
                    .build();
            logEventList.add(tempLogEvent);
        }

        while (failedPost && (failCount < retryCount)) {
            try {
                PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
                        .logEvents(logEventList)
                        .logGroupName(logGroup)
                        .logStreamName(logStream)
                        .build();

                PutLogEventsResponse putLogEventsResponse = cloudWatchLogsClient.putLogEvents(putLogEventsRequest);

                failedPost = false;
            } catch (CloudWatchLogsException e) {
                LOG.error("Failed to push logs with error: {}", e.getMessage());
                LOG.warn("Trying to retransmit request...");
                failCount++;
            }
        }

        if (failedPost) {
            LOG.error("Error, timed out trying to push logs!");
            throw new RuntimeException("Error, timed out trying to push logs! (Max retry_count reached)");
        } else {
            failCount = 0;
        }
    }

    public void shutdown() {
        cloudWatchLogsClient.close();
    }

    //For Testing:
    public void setCloudWatchLogsClient(CloudWatchLogsClient cloudWatchLogsClient) {
        this.cloudWatchLogsClient = cloudWatchLogsClient;
    }
}
