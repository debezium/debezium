/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

public class CloseIncrementalSnapshotWindow<P extends Partition> implements SignalAction<P> {

    public static final String NAME = "snapshot-window-close";

    private final EventDispatcher<P, ? extends DataCollectionId> dispatcher;

    public CloseIncrementalSnapshotWindow(EventDispatcher<P, ? extends DataCollectionId> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public boolean arrived(SignalPayload<P> signalPayload) throws InterruptedException {
        dispatcher.getIncrementalSnapshotChangeEventSource().closeWindow(signalPayload.partition, signalPayload.id,
                signalPayload.offsetContext);
        return true;
    }

}
