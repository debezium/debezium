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
    TRUE, // record is from snapshot is not the last one
    LAST, // record is from snapshot is the last record generated in snapshot phase
    FALSE; // record is from streaming phase

    public static SnapshotRecord fromSource(Struct source) {
        if (source.schema().field(AbstractSourceInfo.SNAPSHOT_KEY) != null && io.debezium.data.Enum.LOGICAL_NAME.equals(source.schema().field(AbstractSourceInfo.SNAPSHOT_KEY).schema().name())) {
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
