/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;

/**
 * Builds SourceRecord objects from envelope components produced by
 * {@link io.debezium.relational.SnapshotChangeRecordEmitter}.
 *
 * <p>Produces standard Debezium envelope records (before/after/source/op/ts_ms),
 * reusing the existing emitter pipeline rather than duplicating format logic.
 *
 * @author Ivan Senyk
 */
public class SnapshotRecordBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotRecordBuilder.class);

    public static SourceRecord buildEnvelopeRecord(
                                                   Object key,
                                                   Struct envelope,
                                                   TableId tableId,
                                                   TableSchema tableSchema) {

        if (envelope == null) {
            LOGGER.warn("Envelope is null for table '{}', skipping row", tableId);
            return null;
        }

        return new SourceRecord(
                createSourcePartition(tableId),
                createSourceOffset(),
                tableId.toString(),
                tableSchema.keySchema(),
                key,
                envelope.schema(),
                envelope);
    }

    private static Map<String, Object> createSourcePartition(TableId tableId) {
        Map<String, Object> partition = new HashMap<>();
        partition.put("server", "snapshot");
        partition.put("database", tableId.catalog());
        return partition;
    }

    private static Map<String, Object> createSourceOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put("snapshot", true);
        offset.put("snapshot_completed", false);
        return offset;
    }
}
