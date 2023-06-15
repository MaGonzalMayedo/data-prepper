package org.opensearch.dataprepper.plugins.sink.buffer;

/**
 * Buffer Factory simply returns a InMemoryBuffer
 * for use during log publishing.
 */
public class InMemoryBufferFactory implements BufferFactory{

    @Override
    public Buffer getBuffer() {
        return new InMemoryBuffer();
    }
}
