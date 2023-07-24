package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsDispatcher;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsService;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.packaging.ThreadTaskEvents;
import org.opensearch.dataprepper.plugins.sink.utils.CloudWatchLogsLimits;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class CloudWatchLogsServiceIT {

    private static final int NUMBER_THREADS_SMALL = 5;
    private static final int NUMBER_THREADS_MEDIUM = 50;
    private static final int NUMBER_THREADS_BIG = 100;
    private static final int NUMBER_THREADS_LARGE = 500;
    private BlockingQueue<ThreadTaskEvents> taskQueue;
    private CloudWatchLogsService cloudWatchLogsService;
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    private ThresholdConfig thresholdConfig;
    private CloudWatchLogsLimits cloudWatchLogsLimits;
    private InMemoryBufferFactory inMemoryBufferFactory;
    private Buffer buffer;
    private CloudWatchLogsDispatcher dispatcher;
    private CloudWatchLogsClient cloudWatchLogsClient;
    private volatile int testCounter;

    @BeforeEach
    void setUp() {
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);

        thresholdConfig = new ThresholdConfig(); //Class can stay as is.
        cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSize(), thresholdConfig.getLogSendInterval());

        taskQueue = new ArrayBlockingQueue<>(CloudWatchLogsSink.BLOCKING_QUEUE_SIZE);
        inMemoryBufferFactory = new InMemoryBufferFactory();
        buffer = inMemoryBufferFactory.getBuffer();
//        dispatcher = new CloudWatchLogsDispatcher();

        cloudWatchLogsService = new CloudWatchLogsService(buffer, cloudWatchLogsLimits, dispatcher, taskQueue);

        testCounter = 0;
    }

    Collection<Record<Event>> getSampleRecordsLess() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecords() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < thresholdConfig.getBatchSize(); i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecordsLarge() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < (thresholdConfig.getBatchSize() * 4); i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    @Test
    void check_dispatcher_run_was_not_called() {
        cloudWatchLogsService.processLogEvents(getSampleRecordsLess());
        verify(dispatcher, never()).run();
    }

    @Test
    void check_dispatcher_run_was_called_test() {
        cloudWatchLogsService.processLogEvents(getSampleRecords());
        verify(dispatcher, atLeastOnce()).run();
    }

    @Test
    void check_dispatcher_run_called_heavy_load() {
        cloudWatchLogsService.processLogEvents(getSampleRecordsLarge());
        verify(dispatcher, atLeast(4)).run();
    }

    //TODO: Add multithreaded testing to ensure that the proper methods (run) gets called.

    @Test
    void test_less_threads_normal_load() {
        Collection<Thread> threadsToRun = new ArrayList<>();

        for (int i = 0; i < NUMBER_THREADS_SMALL; i++) {
            Thread testingThread = new Thread(new CloudWatchLogsServiceIT.CloudWatchLogsServiceTester(getSampleRecords(), cloudWatchLogsService));
            threadsToRun.add(testingThread);
        }

        for (Thread serviceTester: threadsToRun) {
            serviceTester.start();
        }

        for (Thread serviceTester: threadsToRun) {
            try {
                serviceTester.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        verify(dispatcher, atLeast(NUMBER_THREADS_SMALL)).run();
    }

    @Test
    void test_less_threads_heavy_load() {
        Collection<Thread> threadsToRun = new ArrayList<>();

        for (int i = 0; i < NUMBER_THREADS_SMALL; i++) {
            Thread testingThread = new Thread(new CloudWatchLogsServiceIT.CloudWatchLogsServiceTester(getSampleRecordsLarge(), cloudWatchLogsService));
            threadsToRun.add(testingThread);
        }

        for (Thread serviceTester: threadsToRun) {
            serviceTester.start();
        }

        for (Thread serviceTester: threadsToRun) {
            try {
                serviceTester.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        verify(dispatcher, atLeast(NUMBER_THREADS_SMALL * 4)).run();
    }

    @Test
    void test_more_threads_normal_load() {
        Collection<Thread> threadsToRun = new ArrayList<>();

        for (int i = 0; i < NUMBER_THREADS_BIG; i++) {
            Thread testingThread = new Thread(new CloudWatchLogsServiceIT.CloudWatchLogsServiceTester(getSampleRecords(), cloudWatchLogsService));
            threadsToRun.add(testingThread);
        }

        for (Thread serviceTester: threadsToRun) {
            serviceTester.start();
        }

        for (Thread serviceTester: threadsToRun) {
            try {
                serviceTester.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        verify(dispatcher, atLeast(NUMBER_THREADS_BIG)).run();
    }

    @Test
    void test_more_threads_heavy_load() {
        Collection<Thread> threadsToRun = new ArrayList<>();

        for (int i = 0; i < NUMBER_THREADS_BIG; i++) {
            Thread testingThread = new Thread(new CloudWatchLogsServiceIT.CloudWatchLogsServiceTester(getSampleRecordsLarge(), cloudWatchLogsService));
            threadsToRun.add(testingThread);
        }

        for (Thread serviceTester: threadsToRun) {
            serviceTester.start();
        }

        for (Thread serviceTester: threadsToRun) {
            try {
                serviceTester.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        verify(dispatcher, atLeast(NUMBER_THREADS_BIG * 4)).run();
    }

    @Test
    void test_large_threads_normal_load() {
        Collection<Thread> threadsToRun = new ArrayList<>();

        for (int i = 0; i < NUMBER_THREADS_LARGE; i++) {
            Thread testingThread = new Thread(new CloudWatchLogsServiceIT.CloudWatchLogsServiceTester(getSampleRecords(), cloudWatchLogsService));
            threadsToRun.add(testingThread);
        }

        for (Thread serviceTester: threadsToRun) {
            serviceTester.start();
        }

        for (Thread serviceTester: threadsToRun) {
            try {
                serviceTester.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        verify(dispatcher, atLeast(NUMBER_THREADS_LARGE)).run();
    }

    @Test
    void test_large_threads_heavy_load() {
        Collection<Thread> threadsToRun = new ArrayList<>();

        for (int i = 0; i < NUMBER_THREADS_LARGE; i++) {
            Thread testingThread = new Thread(new CloudWatchLogsServiceIT.CloudWatchLogsServiceTester(getSampleRecordsLarge(), cloudWatchLogsService));
            threadsToRun.add(testingThread);
        }

        for (Thread serviceTester: threadsToRun) {
            serviceTester.start();
        }

        for (Thread serviceTester: threadsToRun) {
            try {
                serviceTester.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        verify(dispatcher, atLeast(NUMBER_THREADS_LARGE * 4)).run();
    }

    static class CloudWatchLogsServiceTester implements Runnable {
        Collection<Record<Event>> testEvents;
        CloudWatchLogsService testCloudWatchLogsService;
        CloudWatchLogsServiceTester(Collection<Record<Event>> events, CloudWatchLogsService cloudWatchLogsService) {
            testEvents = events;
            testCloudWatchLogsService = cloudWatchLogsService;
        }

        @Override
        public void run() {
            testCloudWatchLogsService.processLogEvents(testEvents);
        }
    }
}
