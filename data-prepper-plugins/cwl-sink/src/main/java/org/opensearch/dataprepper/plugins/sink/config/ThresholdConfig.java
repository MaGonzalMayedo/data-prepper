package org.opensearch.dataprepper.plugins.sink.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The threshold config holds the different configurations for
 * buffer restrictions, retransmission restrictions and timeout
 * restrictions.
 */
public class ThresholdConfig {
    public static final int DEFAULT_BATCH_SIZE = 10;
    public static final int DEFAULT_EVENT_SIZE = 50;
    public static final int DEFAULT_NUMBER_OF_EVENTS = 10;
    public static final int DEFAULT_RETRY_COUNT = 10;
    public static final int DEFAULT_BACKOFF_TIME = 1000;

    @JsonProperty("batch_size")
    private int batchSize = DEFAULT_BATCH_SIZE;

    @JsonProperty("max_event_size")
    private int maxEventSize = DEFAULT_EVENT_SIZE;

    @JsonProperty("max_number_of_events")
    private int maxEvents = DEFAULT_NUMBER_OF_EVENTS;

    @JsonProperty("retry_count")
    private int retryCount = DEFAULT_RETRY_COUNT;

    @JsonProperty("backoff_time")
    private int backOffTime = DEFAULT_BACKOFF_TIME;

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxEventSize() {
        return maxEventSize;
    }

    public int getMaxEvents() {
        return maxEvents;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getBackOffTime() {
        return backOffTime;
    }
}
