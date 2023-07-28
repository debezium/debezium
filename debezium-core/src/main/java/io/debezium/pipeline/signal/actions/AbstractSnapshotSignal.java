/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Document;
import io.debezium.pipeline.spi.Partition;

/**
 * @author Chris Cranford
 */
public abstract class AbstractSnapshotSignal<P extends Partition> implements SignalAction<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSnapshotSignal.class);
    protected static final String FIELD_DATA_COLLECTIONS = "data-collections";
    protected static final String FIELD_TYPE = "type";
    protected static final String FIELD_ADDITIONAL_CONDITION = "additional-condition";
    protected static final String FIELD_SURROGATE_KEY = "surrogate-key";

    public enum SnapshotType {
        INCREMENTAL,
        BLOCKING
    }

    public static SnapshotType getSnapshotType(Document data) {

        final String typeStr = Optional.ofNullable(data.getString(FIELD_TYPE)).orElse(SnapshotType.INCREMENTAL.toString());
        try {
            return SnapshotType.valueOf(typeStr.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            LOGGER.warn("Detected an unexpected snapshot type '{}'. Signal will be skipped.", typeStr);
            return null;
        }
    }

}
