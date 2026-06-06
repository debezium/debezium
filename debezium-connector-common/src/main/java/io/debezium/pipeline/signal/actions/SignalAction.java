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
}
