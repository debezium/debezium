/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;

/**
 * Builds flat SourceRecord objects from JDBC snapshot rows.
 *
 * <p>Produces records matching the output format of ExtractNewRecordState (unwrap SMT):
 * flat struct with column fields + metadata fields (__op, __table, __source_ts_ms, etc.).
 *
 * <p>This class lives in debezium-connector-common so that TableSnapshotWorker can use it
 * directly without depending on debezium-embedded.
 *
 * @author Ivan Senyk
 */
public class SnapshotRecordBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotRecordBuilder.class);

    public static SourceRecord buildFlatRecord(
                                               Object[] row,
                                               TableId tableId,
                                               TableSchema tableSchema,
                                               OffsetContext offsetContext) {

        Struct after = tableSchema.valueFromColumnData(row);
        Struct key = tableSchema.keyFromColumnData(row);

        if (after == null) {
            LOGGER.warn("valueFromColumnData returned null for table '{}', skipping row", tableId);
            return null;
        }

        SchemaBuilder flatSchemaBuilder = SchemaBuilder.struct();

        for (Field field : after.schema().fields()) {
            flatSchemaBuilder.field(field.name(), field.schema());
        }

        flatSchemaBuilder.field("__op", Schema.OPTIONAL_STRING_SCHEMA);
        flatSchemaBuilder.field("__table", Schema.OPTIONAL_STRING_SCHEMA);
        flatSchemaBuilder.field("__source_ts_ms", Schema.OPTIONAL_INT64_SCHEMA);
        flatSchemaBuilder.field("__source_ts_ns", Schema.OPTIONAL_INT64_SCHEMA);
        flatSchemaBuilder.field("__db", Schema.OPTIONAL_STRING_SCHEMA);
        flatSchemaBuilder.field("__ts_ms", Schema.OPTIONAL_INT64_SCHEMA);
        flatSchemaBuilder.field("__ts_ns", Schema.OPTIONAL_INT64_SCHEMA);
        flatSchemaBuilder.field("__deleted", Schema.OPTIONAL_STRING_SCHEMA);

        Schema flatSchema = flatSchemaBuilder.build();
        Struct flatValue = new Struct(flatSchema);

        for (Field field : after.schema().fields()) {
            flatValue.put(field.name(), after.get(field));
        }

        long nowMs = System.currentTimeMillis();
        long sourceTsMs = nowMs;
        long sourceTsNs = nowMs * 1_000_000L;

        if (offsetContext != null) {
            try {
                Struct sourceInfo = offsetContext.getSourceInfo();
                if (sourceInfo != null) {
                    Object tsMs = sourceInfo.get("ts_ms");
                    if (tsMs instanceof Long) {
                        sourceTsMs = (Long) tsMs;
                        sourceTsNs = sourceTsMs * 1_000_000L;
                    }
                }
            }
            catch (Exception e) {
                LOGGER.debug("Could not extract ts_ms from OffsetContext, using current time");
            }
        }

        flatValue.put("__op", "r");
        flatValue.put("__table", tableId.table());
        flatValue.put("__source_ts_ms", sourceTsMs);
        flatValue.put("__source_ts_ns", sourceTsNs);
        flatValue.put("__db", tableId.catalog() != null ? tableId.catalog() : "");
        flatValue.put("__ts_ms", nowMs);
        flatValue.put("__ts_ns", nowMs * 1_000_000L);
        flatValue.put("__deleted", "false");

        return new SourceRecord(
                createSourcePartition(tableId),
                createSourceOffset(),
                tableId.toString(),
                tableSchema.keySchema(),
                key,
                flatSchema,
                flatValue);
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
