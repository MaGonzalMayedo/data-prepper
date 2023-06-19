package org.opensearch.dataprepper.plugins.sink.buffer;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.time.StopWatch;
import org.checkerframework.checker.units.qual.A;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

//TODO: Can inject a threshold for how much data can be held. (Constructor and variable add-on)
//TODO: Can potentially inject a more suitable byte-array alongside the threshold (above) to recover bytes
/**
 * InMemoryBuffer is a simple memory based queue container
 * for recording information in case an error occurs during log publishing.
 */
public class InMemoryBuffer implements Buffer {
    private static ArrayList<Record<Event>> eventBuffer; //TODO: Change this to Record<Event>
    private final StopWatch stopwatch;
    private int bufferSize; //Tracks the number of events stashed in buffer.

    InMemoryBuffer() {
        if (eventBuffer == null) {
            eventBuffer = new ArrayList<>();
        }
        eventBuffer.clear();
        stopwatch = new StopWatch();
        stopwatch.start();
    }

    @Override
    public int getEventCount() {
        return eventBuffer.size();
    }

    @Override
    public long getDuration() {
        return stopwatch.getTime(TimeUnit.SECONDS);
    }

    /**
     * Write byte array representing a single event into byteArray.
     * @param event Event to be stored in buffer.
     */
    @Override
    public void writeEvent(Record<Event> event) {
        eventBuffer.add(event);
    }

    /**
     * getEvent returns an event (Queue style) and removes it at the same time.
     * @return a Record-Event object that was taken from the front of queue.
     */
    @Override
    public Record<Event> getEvent() {
        return eventBuffer.remove(0);
    }
}
