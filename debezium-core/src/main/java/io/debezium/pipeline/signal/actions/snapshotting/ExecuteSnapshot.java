/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.Value;
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
    private static final String MATCH_ALL_PATTERN = ".*";

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

        List<AdditionalCondition> additionalConditions = getAdditionalConditions(signalPayload.data, type);
        Optional<String> surrogateKey = getSurrogateKey(signalPayload.data);

        LOGGER.info("Requested '{}' snapshot of data collections '{}' with additional conditions '{}' and surrogate key '{}'",
                type, dataCollections, additionalConditions, surrogateKey.orElse("PK of table will be used"));

        SnapshotConfiguration.Builder snapsthoConfigurationBuilder = SnapshotConfiguration.Builder.builder();
        snapsthoConfigurationBuilder.dataCollections(dataCollections);
        snapsthoConfigurationBuilder.surrogateKey(surrogateKey.orElse(""));
        additionalConditions.forEach(snapsthoConfigurationBuilder::addCondition);

        switch (type) {
            case INCREMENTAL:
                if (dispatcher.getIncrementalSnapshotChangeEventSource() == null) {
                    throw new DebeziumException(
                            "Incremental snapshot is not properly configured, either sinalling data collection is not provided or connector-specific snapshotting not set");
                }
                dispatcher.getIncrementalSnapshotChangeEventSource().addDataCollectionNamesToSnapshot(
                        signalPayload, snapsthoConfigurationBuilder.build());
                break;
            case BLOCKING:
                changeEventSourceCoordinator.doBlockingSnapshot(signalPayload.partition, signalPayload.offsetContext, snapsthoConfigurationBuilder.build());
                break;
        }
        return true;
    }

    private List<AdditionalCondition> getAdditionalConditions(Document data, SnapshotType type) {

        // TODO remove in 2.5 release
        Optional<String> oldAdditionalConditionField = getAdditionalCondition(data);
        if (oldAdditionalConditionField.isPresent() && type.equals(SnapshotType.INCREMENTAL)) {
            return List.of(AdditionalCondition.AdditionalConditionBuilder.builder()
                    .dataCollection(Pattern.compile(MATCH_ALL_PATTERN, Pattern.CASE_INSENSITIVE))
                    .filter(oldAdditionalConditionField.orElse(""))
                    .build());
        }

        return Optional.ofNullable(data.getArray(FIELD_ADDITIONAL_CONDITIONS)).orElse(Array.create()).streamValues()
                .map(this::buildAdditionalCondition)
                .collect(Collectors.toList());
    }

    private AdditionalCondition buildAdditionalCondition(Value value) {

        return AdditionalCondition.AdditionalConditionBuilder.builder()
                .dataCollection(Pattern.compile(value.asDocument().getString(FIELD_DATA_COLLECTION), Pattern.CASE_INSENSITIVE))
                .filter(value.asDocument().getString(FIELD_FILTER))
                .build();
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

    /**
     * TODO remove in 2.5 release
     * @deprecated Use {getAdditionalConditions} instead.
     */
    @Deprecated
    public static Optional<String> getAdditionalCondition(Document data) {
        String additionalCondition = data.getString(FIELD_ADDITIONAL_CONDITION);
        return Strings.isNullOrBlank(additionalCondition) ? Optional.empty() : Optional.of(additionalCondition);
    }

    public static Optional<String> getSurrogateKey(Document data) {
        String surrogateKey = data.getString(FIELD_SURROGATE_KEY);
        return Strings.isNullOrBlank(surrogateKey) ? Optional.empty() : Optional.of(surrogateKey);
    }
}
