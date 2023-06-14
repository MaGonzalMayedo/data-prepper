package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;

/**
 * This class implements the CWL-Sink hub component.
 * It binds all the configurations and methods needed
 * to interact with CloudWatchLogs.
 */

@DataPrepperPlugin(name = "cwl-sink", pluginType = Sink.class, pluginConfigurationType = CwlSinkConfig.class)
public class CwlSink implements Sink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(CwlSink.class);
    private final String outputFilePath;
    private final AuthConfig authConfig;
    private final ClientConfig clientConfig;
    private CwlClient cwlClient;
    private final ReentrantLock lock; //Prevents race conditions.
    private boolean isStopRequested;
    private boolean isInitialized;

    @DataPrepperPluginConstructor
    public CwlSink(final CwlSinkConfig cwlSinkConfig) {
        this.outputFilePath = cwlSinkConfig.getPath();
        this.authConfig = cwlSinkConfig.getAuthConfig();
        this.clientConfig = cwlSinkConfig.getClientConfig();
        lock = new ReentrantLock(true);
    }

    @Override
    public void output(Collection<Record<Event>> records) {
        lock.lock();
        try {
            if (isStopRequested)
                return;

            for (final Record<Event> record : records) {
                cwlClient.pushLogs(records);
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() {
        isStopRequested = true;
        lock.lock();
        try {
           cwlClient.shutdown();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void initialize() {
        cwlClient = new CwlClient(clientConfig.getLogGroup(), clientConfig.getLogStream(), clientConfig.getBatchSize(), clientConfig.getRetryCount());
        isInitialized = true;
    }

    @Override
    public boolean isReady() {
        return isInitialized;
    }
}
