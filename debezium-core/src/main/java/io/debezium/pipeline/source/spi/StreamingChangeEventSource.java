/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

/**
 * A change event source that emits events from a DB log, such as MySQL's binlog or similar.
 *
 * @author Gunnar Morling
 */
public interface StreamingChangeEventSource extends ChangeEventSource {

    /**
     * Starts this source. Implementations must return immediately, spawning a separate executor, thread or similar
     * which runs their event handler.
     */
    void start();

    /**
     * Stops this source.
     *
     * @throws InterruptedException in case stopping was interrupted.
     */
    void stop() throws InterruptedException;
}
