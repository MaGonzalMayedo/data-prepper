/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.threshold;

/**
 * ThresholdCheck receives paramaters for which to reference the
 * limits of a buffer and CloudWatchLogsClient before making a
 * PutLogEvent request to AWS.
 */
public class ThresholdCheck {
    private final int batchSize;
    private final int maxEventSizeBytes;
    private final int maxRequestSizeBytes;
    private final long logSendInterval;

    public ThresholdCheck(int batchSize, int maxEventSizeBytes, int maxRequestSizeBytes, int logSendInterval) {
        this.batchSize = batchSize;
        this.maxEventSizeBytes = maxEventSizeBytes;
        this.maxRequestSizeBytes = maxRequestSizeBytes;
        this.logSendInterval = logSendInterval;
    }

    public boolean isThresholdReached(long currentTime, int currentRequestSize, int batchSize) {
        return ((checkBatchSize(batchSize) || checkLogSendInterval(currentTime)
                || checkMaxRequestSize(currentRequestSize)) && (batchSize > 0));
    }

    /**
     * Checks if the interval passed in is equal to or greater
     * than the threshold interval for sending PutLogEvents.
     * @param currentTimeSeconds int denoting seconds.
     * @return boolean - true if greater than or equal to logInterval, false otherwise.
     */
    public boolean checkLogSendInterval(long currentTimeSeconds) {
        return currentTimeSeconds >= logSendInterval;
    }

    /**
     * Determines if the event size is greater than the max event size.
     * @param eventSize int denoting size of event.
     * @return boolean - true if greater than MaxEventSize, false otherwise.
     */
    public boolean checkMaxEventSize(int eventSize) {
        return eventSize > maxEventSizeBytes;
    }

    /**
     * Checks if the request size is greater than or equal to the current size passed in.
     * @param currentRequestSize int denoting size of request(Sum of PutLogEvent messages).
     * @return boolean - true if greater than or equal to the Max request size, smaller otherwise.
     */
    public boolean checkMaxRequestSize(int currentRequestSize) {
        return currentRequestSize >= maxRequestSizeBytes;
    }

    /**
     * Checks if the current batch size is equal to the threshold
     * batch size.
     * @param batchSize int denoting the size of the batch of PutLogEvents.
     * @return boolean - true if equal, false otherwise.
     */
    public boolean checkBatchSize(int batchSize) {
        return batchSize == this.batchSize;
    }
}
