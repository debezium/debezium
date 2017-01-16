/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.consumer;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A stream of change events that can be consumed through polling.
 * 
 * @author Randall Hauch
 */
public interface EventStream extends AutoCloseable {

    /**
     * Obtain the next sequence of change events that are available. This method returns immediately with any available events,
     * though the resulting list of events may be empty.
     * 
     * @return the events that were available; never null but possibly empty
     * @throws InterruptedException if the calling thread is interrupted while waiting for change events
     * @see #poll(long, TimeUnit)
     */
    default List<ChangeEvent> poll() throws InterruptedException {
        return poll(0, TimeUnit.SECONDS);
    }

    /**
     * Obtain the next sequence of change events, optionally blocking for a maximum amount of time until change events become
     * available.
     * 
     * @param timeout the maximum time the call should wait if no events are available in the buffer, or 0 if the method
     *            should return immediately with any available events; must not be negative
     * @param unit the unit of time for the {@code timeout}; may not be null
     * @return the events that were available; never null but possibly empty
     * @throws InterruptedException if the calling thread is interrupted while waiting for change events
     * @see #poll()
     */
    List<ChangeEvent> poll(long timeout, TimeUnit unit) throws InterruptedException;
    
    /**
     * Return whether this stream has been {@link #close() closed}.
     * 
     * @return {@code true} if the stream has been closed and is no longer usable, or {@code false} if the stream has not
     * yet been closed
     */
    boolean isClosed();
}
