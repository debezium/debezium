/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

/**
 * A variant of {@link Runnable} that can be blocked and interrupted.
 */
@FunctionalInterface
public interface BlockingRunnable {

    /**
     * Performs this action.
     *
     * @throws InterruptedException if the calling thread is interrupted while blocking
     */
    void run() throws InterruptedException;
}
