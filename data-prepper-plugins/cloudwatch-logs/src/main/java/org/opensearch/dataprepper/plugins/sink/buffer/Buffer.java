/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.buffer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Buffer that handles the temporary storage of
 * events. It isolates the implementation of system storage.
 */
public interface Buffer {
    /**
     * Size of buffer in events.
     * @return int
     */
    int getEventCount();

    /**
     * Size of buffer in bytes.
     * @return int
     */
    int getBufferSize();

    void writeEvent(byte[] event) throws IOException;

    byte[] getEvent();

    ArrayList<byte[]> getBufferedData();

    void clearBuffer();
}