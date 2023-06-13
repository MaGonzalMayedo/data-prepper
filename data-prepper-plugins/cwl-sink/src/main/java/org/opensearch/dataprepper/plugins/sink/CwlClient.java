package org.opensearch.dataprepper.plugins.sink;

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
    public CwlClient(String logGroup, String logStream, int batchSize, int retryCount) {
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.batchSize = batchSize;
        this.retryCount = retryCount;
    }


}
