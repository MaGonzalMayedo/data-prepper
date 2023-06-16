package org.opensearch.dataprepper.plugins.sink.buffer;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.dataprepper.model.event.Event;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

//TODO: Can inject a threshold for how much data can be held. (Constructor and variable add-on)
//TODO: Can potentially inject a more suitable byte-array alongside the threshold (above) to recover bytes
//TODO: of an event rather the storing the entire event objects.

/**
 * InMemoryBuffer is a simple memory based queue container
 * for recording information in case an error occurs during log publishing.
 */
public class InMemoryBuffer implements Buffer {
    private static ArrayList<Event> eventBuffer; //TODO: Change this to Record<Event>
    private final StopWatch stopwatch;

    InMemoryBuffer() {
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
    public void writeEvent(Event event) {
        eventBuffer.add(event);
    }

    @Override
    public Event getEvent() {
        return eventBuffer.get(0);
    }
}
