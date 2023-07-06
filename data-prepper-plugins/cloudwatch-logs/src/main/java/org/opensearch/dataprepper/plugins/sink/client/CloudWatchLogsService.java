/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import io.micrometer.core.instrument.Counter;
import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.exception.RetransmissionLimitException;
import org.opensearch.dataprepper.plugins.sink.threshold.ThresholdCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedLogEventsInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

//TODO: Need to add e2e testing here to test this the range of the acquired events by CWL.

                /*TODO: Can add DLQ logic here for sending these logs to a particular DLQ for error checking. (Explicitly for bad formatted logs).
                    as currently the logs that are able to be published but rejected by CloudWatch Logs will simply be deleted if not deferred to
                    a backup storage.
                 */
//TODO: Must also consider if the customer makes the logEvent size bigger than the send request size.
//TODO: Can inject another class for the stopWatch functionality.

public class CloudWatchLogsService {
    public static final String NUMBER_OF_RECORDS_PUSHED_TO_CWL_SUCCESS = "cloudWatchLogsEventsSucceeded";
    public static final String NUMBER_OF_RECORDS_PUSHED_TO_CWL_FAIL = "cloudWatchLogsEventsFailed";
    public static final String REQUESTS_SUCCEEDED = "cloudWatchLogsRequestsSucceeded";
    public static final String REQUESTS_FAILED = "cloudWatchLogsRequestsFailed";
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsService.class);
    private final CloudWatchLogsClient cloudWatchLogsClient;
    private final Buffer buffer;
    private final ThresholdCheck thresholdCheck;
    private final List<EventHandle> bufferedEventHandles;
    private final String logGroup;
    private final String logStream;
    private final int retryCount;
    private final long backOffTimeBase;
    private final io.micrometer.core.instrument.Counter logEventSuccessCounter; //Counter to be used on the fly for counting successful transmissions. (Success per single event successfully published).
    private final Counter requestSuccessCount;
    private final io.micrometer.core.instrument.Counter logEventFailCounter;
    private final io.micrometer.core.instrument.Counter requestFailCount; //Counter to be used on the fly during error handling.
    private int failCounter = 0;
    private boolean failedPost;
    private final StopWatch stopWatch;
    private boolean stopWatchOn;

    private ReentrantLock reentrantLock;

    public CloudWatchLogsService(final CloudWatchLogsClient cloudWatchLogsClient, final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig, final Buffer buffer,
                                 final PluginMetrics pluginMetrics, final ThresholdCheck thresholdCheck, final int retryCount, final long backOffTimeBase) {

        this.cloudWatchLogsClient = cloudWatchLogsClient;
        this.buffer = buffer;
        this.logGroup = cloudWatchLogsSinkConfig.getLogGroup();
        this.logStream = cloudWatchLogsSinkConfig.getLogStream();
        this.thresholdCheck = thresholdCheck;

        this.retryCount = retryCount;
        this.backOffTimeBase = backOffTimeBase;

        this.bufferedEventHandles = new ArrayList<>();
        this.logEventSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_PUSHED_TO_CWL_SUCCESS);
        this.requestFailCount = pluginMetrics.counter(REQUESTS_FAILED);
        this.logEventFailCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_PUSHED_TO_CWL_FAIL);
        this.requestSuccessCount = pluginMetrics.counter(REQUESTS_SUCCEEDED);

        reentrantLock = new ReentrantLock();

        stopWatch = StopWatch.create();
        stopWatchOn = false;
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CloudWatchLogs.
     * Implements simple batch limit buffer. (Sends once batch size is reached)
     * @param logs - Collection of Record events which hold log data.
     */
    public void output(final Collection<Record<Event>> logs) {
        reentrantLock.lock();

        if (!stopWatchOn) {
            startStopWatch();
        }

        for (Record<Event> singleLog: logs) {
            String logJsonString = singleLog.getData().toJsonString();
            int logLength = logJsonString.length();

            if (thresholdCheck.checkMaxEventSize(logLength)) {
                LOG.warn("Event blocked due to Max Size restriction!");
                continue;
            }

            //Testing Vars, remove after debugging:
            boolean testBatchCond = thresholdCheck.checkBatchSize(buffer.getEventCount());
            boolean testLogSendInterval = thresholdCheck.checkLogSendInterval(getStopWatchTime());
            boolean testMaxRequestSize = thresholdCheck.checkMaxRequestSize(buffer.getBufferSize() + logLength);
            long stopWatchTime = getStopWatchTime();

            //Conditions for pushingLogs to CWL services:
            if (thresholdCheck.isThresholdReached(getStopWatchTime(), buffer.getBufferSize() + logLength, buffer.getEventCount() + 1)) {
                LOG.info("Attempting to push logs!");
                pushLogs();
                stopAndResetStopWatch();
                startStopWatch();
            }

            if (singleLog.getData().getEventHandle() != null) {
                bufferedEventHandles.add(singleLog.getData().getEventHandle());
            }
            buffer.writeEvent(logJsonString.getBytes());
        }

        runExitCheck();
        reentrantLock.unlock();
    }

    /**
     * pushLogs handles the building of PutLogEvents around a given collection of buffered events.
     * It implements a simple exponential back off time sequence.
     */
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

        while (failedPost && (failCounter < retryCount)) {
            try {
                PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
                        .logEvents(logEventList)
                        .logGroupName(logGroup)
                        .logStreamName(logStream)
                        .build();

                PutLogEventsResponse putLogEventsResponse = cloudWatchLogsClient.putLogEvents(putLogEventsRequest);
                RejectedLogEventsInfo rejectedLogEventsInfo = putLogEventsResponse.rejectedLogEventsInfo();

                if (rejectedLogEventsInfo == null) {
                    requestSuccessCount.increment();
                    failedPost = false;
                    continue;
                }

                printRejectedLogSummary(rejectedLogEventsInfo);

                /*
                    TODO: When a log is rejected by the service, we cannot send it, can probably push to a DLQ here.
                 */

                changeHandleStateRange(rejectedLogEventsInfo.tooNewLogEventStartIndex(), bufferedEventHandles.size(), false);
                changeHandleStateRange(0, Math.max(rejectedLogEventsInfo.tooOldLogEventEndIndex(), rejectedLogEventsInfo.expiredLogEventEndIndex()), false);
                logEventSuccessCounter.increment(Math.max(rejectedLogEventsInfo.tooNewLogEventStartIndex() - Math.max(rejectedLogEventsInfo.tooOldLogEventEndIndex(), rejectedLogEventsInfo.expiredLogEventEndIndex()) - 1, 0));
                bufferedEventHandles.clear();
                failedPost = false;
            } catch (AwsServiceException | SdkClientException e) {
                LOG.error("Failed to push logs with error: {}", e.getMessage());

                try {
                    Thread.sleep(calculateBackOffTime(backOffTimeBase));
                } catch (InterruptedException i) {
                    throw new RuntimeException(i.getMessage());
                }

                LOG.warn("Trying to retransmit request...");
                requestFailCount.increment();
                failCounter += 1;
            }
        }

        if (failedPost) {
            logEventFailCounter.increment(logEventList.size());
            releaseEventHandles(false);
            LOG.error("Error, timed out trying to push logs!");
            throw new RetransmissionLimitException("Error, timed out trying to push logs! (Max retry_count reached)");
        } else {
            logEventSuccessCounter.increment(logEventList.size());
            releaseEventHandles(true);
            failCounter = 0;
        }
    }

    /**
     * Changes the state of the specified range of event handles.
     * Not end inclusive.
     * @param startIndex - int denoting the starting index (inclusive)
     * @param endIndex - int denoting the endibng index (non-inclusive)
     */
    private void changeHandleStateRange(final int startIndex, final int endIndex, final boolean state) {
        if (bufferedEventHandles.size() == 0) {
            return;
        }

        for (int i = startIndex; i < endIndex; i++) {
            bufferedEventHandles.get(i).release(state);
        }
    }

    /**
     * Backoff function that calculates the scaled back off time
     * based on the current attempt count multiplied by backOffTimeBase milliseconds.
     * @return long - The backoff time that represents the new wait time between retries.
     */
    private long calculateBackOffTime(long backOffTimeBase) {
        return failCounter * backOffTimeBase;
    }

    /**
     * One last check to ensure that if we meet the requirements before exiting, we
     * do one last PLE.
     */
    private void runExitCheck() {
        if (thresholdCheck.isExitThresholdReached(buffer.getBufferSize(), buffer.getEventCount())) {
            LOG.info("Attempting to push logs!");
            pushLogs();
            stopAndResetStopWatch();
            startStopWatch();
        }
    }

    private void printRejectedLogSummary(RejectedLogEventsInfo rejectedLogEventsInfo) {
        LOG.warn("Some logs were rejected!");
        LOG.warn("Too new log event start index: " + rejectedLogEventsInfo.tooNewLogEventStartIndex());
        LOG.warn("Too old log  event end index: " + rejectedLogEventsInfo.tooOldLogEventEndIndex());
        LOG.warn("Expired log event end index: " + rejectedLogEventsInfo.expiredLogEventEndIndex());
    }

    private void releaseEventHandles(final boolean result) {
        if (bufferedEventHandles.size() == 0) {
            return;
        }

        for (EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }

        bufferedEventHandles.clear();
    }

    private void clearEventHandles() {
        bufferedEventHandles.clear();
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
