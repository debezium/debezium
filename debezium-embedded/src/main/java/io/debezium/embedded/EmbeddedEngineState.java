/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The running state of the engine.  The instance of this class should be shared with any objects that need to know if the
 * engine is still running.
 */
public class EmbeddedEngineState {
    private final AtomicReference<Thread> runningThread = new AtomicReference<>();

    /**
     * Mark the engine as running.
     */
    public void start() {
        this.runningThread.set(Thread.currentThread());
    }

    /**
     * Returns true if the engine has been started.
     */
    public boolean isRunning() {
        return this.runningThread.get() != null;
    }

    /**
     * Returns true if the engine has been stopped.
     */
    public boolean isStopped() {
        return this.runningThread.get() == null;
    }

    /**
     * Mark the engine as no longer running.
     */
    public Thread stop() {
        return this.runningThread.getAndSet(null);
    }
}
