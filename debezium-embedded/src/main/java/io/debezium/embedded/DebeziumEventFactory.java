/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

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
 * Factory to create Debezium change events from JDBC ResultSet rows.
 *
 * This allows us to reuse the tested EventConverter code from memiiso/debezium-server-iceberg
 * for handling snapshot data, ensuring consistent null handling and type conversion.
 *
 * @author Ivan Senyk
 */
public class DebeziumEventFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumEventFactory.class);

    /**
     * Creates a Debezium "after" Struct from a JDBC row using Debezium's built-in value conversion.
     *
     * This method delegates to TableSchema's valueGenerator which properly handles:
     * - Type conversion from JDBC types (java.sql.Timestamp, etc.) to Debezium logical types
     * - Null value handling according to schema optionality
     * - Column mapping and filtering
     * - Error handling with proper logging
     *
     * This is the correct approach - we reuse Debezium's tested conversion infrastructure
     * instead of trying to replicate it.
     *
     * @param row JDBC row data (Object[] from ResultSet)
     * @param tableSchema Debezium table schema with configured ValueConverters
     * @return Debezium Struct representing the row data with properly converted values
     */
    public static Struct createDebeziumStruct(Object[] row, TableSchema tableSchema) {
        // Use TableSchema's valueGenerator which has ValueConverters already configured
        // This properly converts JDBC types (java.sql.Timestamp, etc.) to Debezium logical types
        // (io.debezium.time.Timestamp, IsoTimestamp, etc.)
        return tableSchema.valueFromColumnData(row);
    }

    /**
     * Creates a Debezium change event representing a snapshot read operation.
     *
     * Produces a FLAT value struct that mimics the output of ExtractNewRecordState (unwrap) SMT.
     * CDC events pass through the unwrap transform which flattens the Debezium envelope
     * (before/after/source/op/ts_ms) into a flat struct of column data + metadata fields
     * (__op, __source_ts_ms, etc.). Snapshot events bypass the SMT pipeline, so we must
     * produce the same flat format here to ensure StructEventConverter can map columns correctly.
     *
     * @param row JDBC row data (Object[] from ResultSet)
     * @param tableId Table identifier
     * @param tableSchema Debezium table schema
     * @param offsetContext Offset context with connector state (may be null)
     * @return EmbeddedEngineChangeEvent with flat value struct matching unwrap SMT output
     */
    public static EmbeddedEngineChangeEvent createSnapshotChangeEvent(
                                                                      Object[] row,
                                                                      TableId tableId,
                                                                      TableSchema tableSchema,
                                                                      OffsetContext offsetContext) {

        // Use TableSchema's generators for both value and key
        // These have proper ValueConverters configured for JDBC → Debezium type conversion
        Struct after = tableSchema.valueFromColumnData(row);
        Struct key = tableSchema.keyFromColumnData(row);

        if (after == null) {
            LOGGER.warn("valueFromColumnData returned null for table '{}', skipping row", tableId);
            return null;
        }

        // Build a flat schema: all column fields from 'after' + metadata fields
        // This matches what ExtractNewRecordState would produce with:
        // add.fields=op,table,source.ts_ms,source.ts_ns,db,ts_ms,ts_ns
        // delete.handling.mode=rewrite
        SchemaBuilder flatSchemaBuilder = SchemaBuilder.struct();

        // Add all table column fields from the after struct
        for (Field field : after.schema().fields()) {
            flatSchemaBuilder.field(field.name(), field.schema());
        }

        // Add metadata fields that ExtractNewRecordState would add
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

        // Copy all column values from after struct
        for (Field field : after.schema().fields()) {
            flatValue.put(field.name(), after.get(field));
        }

        // Set metadata values
        long nowMs = System.currentTimeMillis();
        long sourceTsMs = nowMs;
        long sourceTsNs = nowMs * 1_000_000L;

        // Use OffsetContext source timestamp if available (more accurate)
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

        flatValue.put("__op", "r"); // 'r' = read (snapshot)
        flatValue.put("__table", tableId.table());
        flatValue.put("__source_ts_ms", sourceTsMs);
        flatValue.put("__source_ts_ns", sourceTsNs);
        flatValue.put("__db", tableId.catalog() != null ? tableId.catalog() : "");
        flatValue.put("__ts_ms", nowMs);
        flatValue.put("__ts_ns", nowMs * 1_000_000L);
        flatValue.put("__deleted", "false");

        // Create SourceRecord with FLAT value (not envelope)
        // This matches the format StructEventConverter expects after ExtractNewRecordState
        SourceRecord sourceRecord = new SourceRecord(
                createSourcePartition(tableId),
                createSourceOffset(),
                tableId.toString(),
                tableSchema.keySchema(),
                key,
                flatSchema,
                flatValue);

        return new EmbeddedEngineChangeEvent<>(null, null, null, sourceRecord);
    }

    /**
     * Wraps a SourceRecord into an EmbeddedEngineChangeEvent.
     */
    public static EmbeddedEngineChangeEvent wrapSourceRecord(SourceRecord record) {
        return new EmbeddedEngineChangeEvent<>(null, null, null, record);
    }

    /**
     * Creates source partition metadata.
     */
    private static Map<String, Object> createSourcePartition(TableId tableId) {
        Map<String, Object> partition = new HashMap<>();
        partition.put("server", "snapshot");
        partition.put("database", tableId.catalog());
        return partition;
    }

    /**
     * Creates source offset metadata.
     */
    private static Map<String, Object> createSourceOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put("snapshot", true);
        offset.put("snapshot_completed", false);
        return offset;
    }
}
