/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.Signal.Payload;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The action to trigger an ad-hoc snapshot.
 * The action parameters are {@code type} of snapshot and list of {@code data-collections} on which the
 * snapshot will be executed.
 *
 * @author Jiri Pechanec
 *
 */
public class ExecuteSnapshot<P extends Partition> extends AbstractSnapshotSignal<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteSnapshot.class);

    public static final String NAME = "execute-snapshot";

    private final EventDispatcher<P, ? extends DataCollectionId> dispatcher;

    public ExecuteSnapshot(EventDispatcher<P, ? extends DataCollectionId> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public boolean arrived(Payload<P> signalPayload) throws InterruptedException {
        final List<String> dataCollections = getDataCollections(signalPayload.data);
        if (dataCollections == null) {
            return false;
        }
        SnapshotType type = getSnapshotType(signalPayload.data);
        Optional<String> additionalCondition = getAdditionalCondition(signalPayload.data);
        LOGGER.info("Requested '{}' snapshot of data collections '{}' with additional condition '{}'", type, dataCollections,
                additionalCondition.orElse("No condition passed"));
        switch (type) {
            case INCREMENTAL:
                dispatcher.getIncrementalSnapshotChangeEventSource().addDataCollectionNamesToSnapshot(
                        signalPayload.partition, dataCollections, additionalCondition, signalPayload.offsetContext);
                break;
        }
        return true;
    }

    public static List<String> getDataCollections(Document data) {
        final Array dataCollectionsArray = data.getArray(FIELD_DATA_COLLECTIONS);
        if (dataCollectionsArray == null || dataCollectionsArray.isEmpty()) {
            LOGGER.warn(
                    "Execute snapshot signal '{}' has arrived but the requested field '{}' is missing from data or is empty",
                    data, FIELD_DATA_COLLECTIONS);
            return null;
        }
        return dataCollectionsArray.streamValues()
                .map(v -> v.asString().trim())
                .collect(Collectors.toList());
    }

    public static Optional<String> getAdditionalCondition(Document data) {
        String additionalCondition = data.getString(FIELD_ADDITIONAL_CONDITION);
        return (additionalCondition == null || additionalCondition.trim().isEmpty()) ? Optional.empty() : Optional.of(additionalCondition);
    }
}
