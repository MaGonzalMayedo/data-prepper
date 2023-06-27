package org.opensearch.dataprepper.plugins.sink;

import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.threshold.ThresholdCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class CwlClient {
    private static final Logger LOG = LoggerFactory.getLogger(CwlClient.class);
    private final CloudWatchLogsClient cloudWatchLogsClient;
    private final Buffer buffer;
    private final ThresholdCheck thresholdCheck;
    private final String logGroup;
    private final String logStream;
    private final int maxLogsQueued = 0; //TODO: Make use of this parameter if needed.
    private final int retryCount;
    private int failCount = 0; //Counter to be used on the fly during error handling.
    private boolean failedPost;
    private final StopWatch stopWatch;
    private boolean stopWatchOn;

    CwlClient(final CloudWatchLogsClient cloudWatchLogsClient, final Buffer buffer,
              final ThresholdCheck thresholdCheck, String logGroup, final String logStream, final int batchSize, final int retryCount) {

        this.cloudWatchLogsClient = cloudWatchLogsClient;
        this.buffer = buffer;
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.thresholdCheck = thresholdCheck;
        this.retryCount = retryCount;
        stopWatch = StopWatch.create();
        stopWatchOn = false;
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CWL.
     * Implements simple batch limit buffer. (Sends once batch size is reached)
     * @param logs Collection of Record events which hold log data.
     */
    public void output(final Collection<Record<Event>> logs) {
        for (Record<Event> singleLog: logs) {
            String logJsonString = singleLog.getData().toJsonString();
            int logLength = logJsonString.length();

            if (!stopWatchOn) {
                startStopWatch();
            }

            if (thresholdCheck.checkMaxEventSize(logLength)) {
                LOG.warn("Event blocked due to Max Size restriction!");
                continue;
            }

            //Conditions for pushingLogs to CWL services:
            if (thresholdCheck.checkBatchSize(buffer.getEventCount()) || thresholdCheck.checkLogSendInterval(getStopWatchTime())
                    || thresholdCheck.checkMaxRequestSize(buffer.getBufferSize())) {
                LOG.info("Attempting to push logs!");
                pushLogs();
                stopStopWatch();
            }

            buffer.writeEvent(logJsonString.getBytes());
        }
    }

    private void pushLogs() {
        ArrayList<InputLogEvent> logEventList = new ArrayList<>();
        failedPost = true;

        while (buffer.getEventCount() > 0) {
            InputLogEvent tempLogEvent = InputLogEvent.builder()
                    .message(new String(buffer.getEvent()))
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

                //Insert logic for resending the logEvents that did not work and retry attempts.

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

    private void startStopWatch() {
        stopWatchOn = true;
        stopWatch.start();
    }

    private void stopStopWatch() {
        stopWatchOn = false;
        stopWatch.stop();
        stopWatch.reset();
    }

    private long getStopWatchTime() {
        return stopWatch.getTime(TimeUnit.SECONDS);
    }
}
