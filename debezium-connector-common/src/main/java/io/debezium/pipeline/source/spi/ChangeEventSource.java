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

        /**
         * Called to indicate that the snapshot has been completed and that streaming should therefore continue.
         */
        void resumeStreaming() throws InterruptedException;

        /**
         * Wait for the resumeStreaming function to be called, which indicates that a snapshot is done
         * and that streaming should resume.
         */
        void waitSnapshotCompletion() throws InterruptedException;

        /**
         * Called by the StreamingChangeEventSource to indicate that the streaming has now been paused, and
         * that no streaming records are being processed anymore.
         */
        void streamingPaused();

        /**
         * Wait for the streamingPaused function to be called.
         */
        void waitStreamingPaused() throws InterruptedException;

        /**
         * Executes any pending signals whose actions requested synchronous invocation.
         * <p>
         * A streaming source calls this from its own thread at points where it is safe for a signal action
         * to inspect or mutate the source's state, for example between batches of events. Signals that
         * arrived since the previous call are executed in order before this method returns.
         *
         * @see io.debezium.pipeline.signal.actions.SignalAction#isSynchronous()
         */
        void processSynchronousSignals() throws InterruptedException;
    }
}
