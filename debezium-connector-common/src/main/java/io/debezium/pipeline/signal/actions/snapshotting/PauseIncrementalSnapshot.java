/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.AbstractSnapshotSignal;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

public class PauseIncrementalSnapshot<P extends Partition> extends AbstractSnapshotSignal<P> {

    public static final String NAME = "pause-snapshot";

    private final EventDispatcher<P, ? extends DataCollectionId> dispatcher;

    public PauseIncrementalSnapshot(EventDispatcher<P, ? extends DataCollectionId> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public boolean arrived(SignalPayload<P> signalPayload) throws InterruptedException {
        dispatcher.getIncrementalSnapshotChangeEventSource().pauseSnapshot(signalPayload.partition, signalPayload.offsetContext);
        return true;
    }

}
