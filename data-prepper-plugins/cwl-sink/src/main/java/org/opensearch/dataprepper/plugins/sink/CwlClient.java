package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.cloudwatch.endpoints.internal.Value;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClientBuilder;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * CwlClient is an interface class that simplifies method calls to post logs to
 * CloudWatch logs. It receives a collection of events and interprets them into
 * a message to send to CWL.
 * TODO: Need to add Client Configuration class. (ARN roles and region bounds)
 * TODO: Add the CloudWatchLogs Agent.
 */
public class CwlClient {
    private final String logGroup;
    private final String logStream;
    private final int batchSize;
    private final int retryCount;
    private final CloudWatchLogsClient cloudWatchLogsClient;


    public CwlClient(String logGroup, String logStream, int batchSize, int retryCount) {
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.batchSize = batchSize;
        this.retryCount = retryCount;
        cloudWatchLogsClient = CloudWatchLogsClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request:
     * @param logs Collection of Record events which hold log data.
     */
    public void pushLogs(Collection<Record<Event>> logs) {

        ArrayList<InputLogEvent> logList = new ArrayList<>();

        for (Record<Event> singleLog: logs) {
            InputLogEvent tempLogEvent = InputLogEvent.builder()
                    .message(singleLog.getData().toJsonString())
                    .timestamp(System.currentTimeMillis())
                    .build();
            logList.add(tempLogEvent);
        }

        try {
            //TODO: Can place this in a different method for clarity.
            //TODO: Add error handling when implementing error handling.
            PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
                    .logEvents(logList)
                    .logGroupName(logGroup)
                    .logStreamName(logStream)
                    .build();

            cloudWatchLogsClient.putLogEvents(putLogEventsRequest); //TODO: Can maybe use a RESPONSE object here.
        } catch (CloudWatchLogsException e) {
            throw new RuntimeException(e.awsErrorDetails().errorMessage(), e);
        }

    }

    public void shutdown() {
        cloudWatchLogsClient.close();
    }
}
