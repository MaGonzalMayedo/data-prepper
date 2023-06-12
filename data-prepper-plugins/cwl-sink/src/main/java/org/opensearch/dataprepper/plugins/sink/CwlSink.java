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
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample-output.txt";

    public static final String FILE_PATH = "path";

    private final String outputFilePath;
    private AuthConfig authConfig;
    private ClientConfig clientConfig;

    private BufferedWriter writer;
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
                try {
                    record.getData().put("log-groupName", this.clientConfig.getLogGroup()); //Testing that the config class works!
                    postEvent(record.getData(), writer);
                } catch (final IOException ex) {
                    throw new RuntimeException(format("Encountered exception writing to file %s", outputFilePath), ex);
                }
            }

            try {
                writer.flush();
            } catch (final IOException ex) {
                LOG.warn("Failed to flush for file {}", outputFilePath, ex);
            }
        } finally {
            lock.unlock();
        }
    }

    public void postEvent(Event event, BufferedWriter writer) throws IOException {
        writer.write(event.toJsonString());
        writer.newLine();
    }

    @Override
    public void shutdown() {
        isStopRequested = true;
        lock.lock();
        try {
            writer.close();
        } catch (final IOException ex) {
            LOG.error("Failed to close file {}.", outputFilePath, ex);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void initialize() {
        try {
            writer = Files.newBufferedWriter(Paths.get(outputFilePath), StandardCharsets.UTF_8);
        } catch (final IOException ex) {
            throw new RuntimeException(format("Encountered exception opening/creating file %s", outputFilePath), ex);
        }
        isInitialized = true;
    }

    @Override
    public boolean isReady() {
        return isInitialized;
    }
}
