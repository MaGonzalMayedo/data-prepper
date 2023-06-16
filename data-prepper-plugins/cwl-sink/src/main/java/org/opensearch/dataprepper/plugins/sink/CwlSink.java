package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.concurrent.locks.ReentrantLock;

//TODO: Add buffer variable to pass into the CwlClient.java

/**
 * This class implements the CWL-Sink hub component.
 * It binds all the configurations and methods needed
 * to interact with CloudWatchLogs.
 */

@DataPrepperPlugin(name = "cwl-sink", pluginType = Sink.class, pluginConfigurationType = CwlSinkConfig.class)
public class CwlSink implements Sink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(CwlSink.class);
    private final AuthConfig authConfig;
    private final ClientConfig clientConfig;
    private CwlClient cwlClient;
    private BufferFactory bufferFactory;
    private final ReentrantLock lock;
    private boolean isStopRequested;
    private boolean isInitialized;

    @DataPrepperPluginConstructor
    public CwlSink(final CwlSinkConfig cwlSinkConfig) {
        this.authConfig = cwlSinkConfig.getAuthConfig();
        this.clientConfig = cwlSinkConfig.getClientConfig();
        lock = new ReentrantLock(true);

        if (clientConfig.getBufferType().equals("in_memory")) {
            bufferFactory = new InMemoryBufferFactory();
        }
    }

    @Override
    public void output(final Collection<Record<Event>> records) {
        lock.lock();
        try {
            if (isStopRequested)
                return;
            LOG.info("Attempting to publish records to CloudWatchLogs");
            cwlClient.output(records);
        } catch (Exception e) {
            LOG.error("Error attempting to push logs! (Max retry_count reached)");
            shutdown();
            throw new RuntimeException();
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
        cwlClient = new CwlClient(bufferFactory.getBuffer() ,clientConfig.getLogGroup(), clientConfig.getLogStream(), clientConfig.getBatchSize(), clientConfig.getRetryCount());
        isInitialized = true;
    }

    @Override
    public boolean isReady() {
        return isInitialized;
    }
}
