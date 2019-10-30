/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

import org.apache.kafka.connect.data.Struct;

/**
 * Describes whether the change record comes from snapshot and if it is the last one
 *
 * @author Jiri Pechanec
 *
 */
public enum SnapshotRecord {
    /**
     * Record is from snapshot is not the last one.
     */
    TRUE,
    /**
     * Record is from snapshot is the last record generated in snapshot phase.
     */
    LAST,
    /**
     * Record is from streaming phase.
     */
    FALSE;

    public static SnapshotRecord fromSource(Struct source) {
        if (source.schema().field(AbstractSourceInfo.SNAPSHOT_KEY) != null
                && io.debezium.data.Enum.LOGICAL_NAME.equals(source.schema().field(AbstractSourceInfo.SNAPSHOT_KEY).schema().name())) {
            final String snapshotString = source.getString(AbstractSourceInfo.SNAPSHOT_KEY);
            if (snapshotString != null) {
                return SnapshotRecord.valueOf(snapshotString.toUpperCase());
            }
        }
        return null;
    }

    public void toSource(Struct source) {
        if (this != SnapshotRecord.FALSE) {
            source.put(AbstractSourceInfo.SNAPSHOT_KEY, name().toLowerCase());
        }
    }
}
