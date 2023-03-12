/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.spi.schema.DataCollectionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.signal.Signal;
import io.debezium.pipeline.signal.Signal.Payload;
import io.debezium.pipeline.spi.Partition;

public class OpenIncrementalSnapshotWindow<P extends Partition> implements Signal.Action<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenIncrementalSnapshotWindow.class);

    public static final String NAME = "snapshot-window-open";
    private final EventDispatcher<P, ? extends DataCollectionId> dispatcher;

    public OpenIncrementalSnapshotWindow(EventDispatcher<P, ? extends DataCollectionId> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public boolean arrived(Payload<P> signalPayload) throws InterruptedException {
        dispatcher.getIncrementalSnapshotChangeEventSource()
                .openWindow(signalPayload.partition, signalPayload.id, signalPayload.data.get("collection").asString(), signalPayload.offsetContext);
        return true;
    }

}
