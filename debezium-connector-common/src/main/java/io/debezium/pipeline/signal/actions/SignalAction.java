/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions;

import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.spi.Partition;

@FunctionalInterface
public interface SignalAction<P extends Partition> {

    /**
     * @param signalPayload the content of the signal
     * @return true if the signal was processed
     */
    boolean arrived(SignalPayload<P> signalPayload) throws InterruptedException;

    /**
     * Indicates whether this action must be invoked synchronously with the streaming source.
     * The streaming thread determines when it is safe to process such signals, rather than they invoked
     * on the thread that received the signal.
     * <p>
     * By default, a signal is executed as soon as it is read, either on the signal processor's executor
     * thread or, for the source channel, on the streaming thread in the middle of dispatching an event.
     * Actions that inspect or mutate state owned by the streaming source should return {@code true}.
     * The {@link io.debezium.pipeline.signal.SignalProcessor} then queues the signal and executes it the
     * next time the streaming source calls
     * {@link io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext#processSynchronousSignals()}.
     * A synchronous signal is never executed unless the streaming source invokes that hook, so an action
     * should only opt in when the connector's streaming source supports it.
     *
     * @return {@code true} if the action must run on the streaming thread, {@code false} otherwise
     */
    default boolean isSynchronous() {
        return false;
    }
}
