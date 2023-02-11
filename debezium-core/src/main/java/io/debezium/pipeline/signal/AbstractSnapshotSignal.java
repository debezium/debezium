/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Document;
import io.debezium.pipeline.spi.Partition;

/**
 * @author Chris Cranford
 */
public abstract class AbstractSnapshotSignal<P extends Partition> implements Signal.Action<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSnapshotSignal.class);
    protected static final String FIELD_DATA_COLLECTIONS = "data-collections";
    protected static final String FIELD_TYPE = "type";
    protected static final String FIELD_ADDITIONAL_CONDITION = "additional-condition";

    public enum SnapshotType {
        INCREMENTAL
    }

    public static SnapshotType getSnapshotType(Document data) {
        final String typeStr = data.getString(FIELD_TYPE);
        SnapshotType type = SnapshotType.INCREMENTAL;
        if (typeStr != null) {
            for (SnapshotType option : SnapshotType.values()) {
                if (option.name().equalsIgnoreCase(typeStr)) {
                    return option;
                }
            }
            LOGGER.warn("Detected an unexpected snapshot type '{}'", typeStr);
            return null;
        }
        return type;
    }

}
