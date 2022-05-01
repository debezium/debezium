/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The action to stop an ad-hoc snapshot.
 *
 * The action parameters are {@code type} of snapshot and optional list of {@code data-collections} on which
 * the snapshot will be stopped for.  If the {@code data-collections} is empty or not provided, the entire
 * ad-hoc snapshot will be stopped.
 *
 * @author Chris Cranford
 */
public class StopSnapshot<P extends Partition> extends AbstractSnapshotSignal<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StopSnapshot.class);

    public static final String NAME = "stop-snapshot";

    private final EventDispatcher<P, ? extends DataCollectionId> dispatcher;

    public StopSnapshot(EventDispatcher<P, ? extends DataCollectionId> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public boolean arrived(Signal.Payload<P> signalPayload) throws InterruptedException {
        final List<String> dataCollections = getDataCollections(signalPayload.data);
        final SnapshotType type = getSnapshotType(signalPayload.data);

        LOGGER.info("Requested stop of snapshot '{}' for data collections '{}'", type, dataCollections);
        switch (type) {
            case INCREMENTAL:
                dispatcher.getIncrementalSnapshotChangeEventSource()
                        .stopSnapshot(signalPayload.partition, dataCollections, signalPayload.offsetContext);
                break;
        }

        return true;
    }

    public static List<String> getDataCollections(Document data) {
        final Array dataCollectionsArray = data.getArray(FIELD_DATA_COLLECTIONS);
        if (dataCollectionsArray == null || dataCollectionsArray.isEmpty()) {
            return null;
        }
        return dataCollectionsArray.streamValues()
                .map(v -> v.asString().trim())
                .collect(Collectors.toList());
    }
}
