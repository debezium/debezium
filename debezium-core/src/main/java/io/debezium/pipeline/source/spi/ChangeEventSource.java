/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

public interface ChangeEventSource {

    interface ChangeEventSourceContext {

        /**
         * Whether this source is paused.
         */
        boolean isPaused();

        /**
         * Whether this source is running or has been requested to stop.
         */
        boolean isRunning();

        void resumeStreaming() throws InterruptedException;

        void waitSnapshotCompletion() throws InterruptedException;

        void streamingPaused();

        void waitStreamingPaused() throws InterruptedException;
    }
}
