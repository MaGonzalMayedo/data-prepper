package org.opensearch.dataprepper.plugins.sink;

import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.threshold.ThresholdCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

//TODO: Need to add e2e testing here to test this the range of the acquired events by CWL.

public class CwlClient {
    private static final Logger LOG = LoggerFactory.getLogger(CwlClient.class);
    private final CloudWatchLogsClient cloudWatchLogsClient;
    private final Buffer buffer;
    private final ThresholdCheck thresholdCheck;
    private final String logGroup;
    private final String logStream;
    private final int retryCount;
    private int logEventSuccessCounter = 0; //Counter to be used on the fly for counting successful transmissions. (Success per single event successfully published).
    private int successCount = 0;
    private int logEventFailCounter = 0;
    private int failCount = 0; //Counter to be used on the fly during error handling.
    private final int backOffTime;
    private boolean failedPost;
    private final StopWatch stopWatch;
    private boolean stopWatchOn;

    CwlClient(final CloudWatchLogsClient cloudWatchLogsClient, final Buffer buffer,
              final ThresholdCheck thresholdCheck, String logGroup, final String logStream, final int batchSize, final int retryCount, final int backOffTime) {

        this.cloudWatchLogsClient = cloudWatchLogsClient;
        this.buffer = buffer;
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.thresholdCheck = thresholdCheck;
        this.retryCount = retryCount;
        this.backOffTime = backOffTime;
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
                    || thresholdCheck.checkMaxRequestSize(buffer.getBufferSize() + logLength)) {
                LOG.info("Attempting to push logs!");
                pushLogs();
                stopAndResetStopWatch();
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
                RejectedLogEventsInfo rejectedLogEventsInfo = putLogEventsResponse.rejectedLogEventsInfo();

                if (rejectedLogEventsInfo == null) {
                    successCount++;
                    logEventSuccessCounter += logEventList.size();
                    failedPost = false;
                    continue;
                }

                LOG.warn("Some logs were rejected!");
                LOG.warn("Too new log event start index: " + rejectedLogEventsInfo.tooNewLogEventStartIndex());
                LOG.warn("Too old log  event end index: " + rejectedLogEventsInfo.tooOldLogEventEndIndex());
                LOG.warn("Expired log event end index: " + rejectedLogEventsInfo.expiredLogEventEndIndex());

                /*TODO: Can add DLQ logic here for sending these logs to a particular DLQ for error checking. (Explicitly for bad formatted logs).
                    as currently the logs that are able to be published but rejected by CloudWatch Logs will simply be deleted if not deferred to
                    a backup storage.
                 */

                logEventSuccessCounter += Math.max(rejectedLogEventsInfo.tooNewLogEventStartIndex() - Math.max(rejectedLogEventsInfo.tooOldLogEventEndIndex(), rejectedLogEventsInfo.expiredLogEventEndIndex()) - 1, 0);
                failedPost = false;
            } catch (CloudWatchLogsException e) {
                LOG.error("Failed to push logs with error: {}", e.getMessage());

                try {
                    Thread.sleep(calculateBackOffTime(backOffTime));
                } catch (InterruptedException i) {
                    throw new RuntimeException(i.getMessage());
                }

                LOG.warn("Trying to retransmit request...");
                logEventFailCounter += logEventList.size();
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

    private long calculateBackOffTime(long backOffTime) {
        return ((long) Math.pow(2, failCount)) * 500;
    }

    public void shutdown() {
        cloudWatchLogsClient.close();
    }

    private void startStopWatch() {
        stopWatchOn = true;
        stopWatch.start();
    }

    private void stopAndResetStopWatch() {
        stopWatchOn = false;
        stopWatch.stop();
        stopWatch.reset();
    }

    private long getStopWatchTime() {
        return stopWatch.getTime(TimeUnit.SECONDS);
    }
}
