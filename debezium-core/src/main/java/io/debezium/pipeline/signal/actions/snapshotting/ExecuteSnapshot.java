/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.AbstractSnapshotSignal;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Strings;

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
    private final ChangeEventSourceCoordinator<P, ? extends OffsetContext> changeEventSourceCoordinator;

    public ExecuteSnapshot(EventDispatcher<P, ? extends DataCollectionId> dispatcher, ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator) {
        this.dispatcher = dispatcher;
        this.changeEventSourceCoordinator = changeEventSourceCoordinator;
    }

    @Override
    public boolean arrived(SignalPayload<P> signalPayload) throws InterruptedException {
        final List<String> dataCollections = getDataCollections(signalPayload.data);
        if (dataCollections == null) {
            return false;
        }
        SnapshotType type = getSnapshotType(signalPayload.data);
        Optional<String> additionalCondition = getAdditionalCondition(signalPayload.data);
        Optional<String> surrogateKey = getSurrogateKey(signalPayload.data);
        LOGGER.info("Requested '{}' snapshot of data collections '{}' with additional condition '{}' and surrogate key '{}'", type, dataCollections,
                additionalCondition.orElse("No condition passed"), surrogateKey.orElse("PK of table will be used"));

        switch (type) {
            case INCREMENTAL:
                dispatcher.getIncrementalSnapshotChangeEventSource().addDataCollectionNamesToSnapshot(
                        signalPayload, dataCollections, additionalCondition, surrogateKey);
                break;
            case BLOCKING:
                changeEventSourceCoordinator.doBlockingSnapshot(signalPayload.partition, signalPayload.offsetContext);
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
        return Strings.isNullOrBlank(additionalCondition) ? Optional.empty() : Optional.of(additionalCondition);
    }

    public static Optional<String> getSurrogateKey(Document data) {
        String surrogateKey = data.getString(FIELD_SURROGATE_KEY);
        return Strings.isNullOrBlank(surrogateKey) ? Optional.empty() : Optional.of(surrogateKey);
    }
}
