/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.embedded;

import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory to create Debezium change events from JDBC ResultSet rows.
 *
 * This allows us to reuse the tested EventConverter code from memiiso/debezium-server-iceberg
 * for handling snapshot data, ensuring consistent null handling and type conversion.
 *
 * @author Debezium Community
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
     * Uses OffsetContext to create a proper source struct with connector-specific metadata.
     * This is the correct Debezium architecture - OffsetContext contains all state (LSN, txId, etc.)
     * and generates the source struct automatically.
     *
     * @param row JDBC row data (Object[] from ResultSet)
     * @param tableId Table identifier
     * @param tableSchema Debezium table schema
     * @param offsetContext Offset context with connector state (may be null for fallback)
     * @return EmbeddedEngineChangeEvent that can be processed by EventConverter
     */
    public static EmbeddedEngineChangeEvent createSnapshotChangeEvent(
            Object[] row,
            TableId tableId,
            TableSchema tableSchema,
            OffsetContext offsetContext) {

        // Use TableSchema's generators for both value and key
        // These have proper ValueConverters configured
        Struct after = tableSchema.valueFromColumnData(row);
        Struct key = tableSchema.keyFromColumnData(row);

        // Get source struct from OffsetContext (proper architecture)
        // If offsetContext is not available, fall back to manual creation
        Struct source;
        if (offsetContext != null) {
            // CORRECT WAY: Use OffsetContext.getSourceInfo() which returns the source Struct
            // See RelationalChangeRecordEmitter.java line 72
            source = offsetContext.getSourceInfo();
        } else {
            // Fallback for backward compatibility: create minimal source struct
            Schema envelopeSchema = tableSchema.getEnvelopeSchema().schema();
            Schema sourceSchema = envelopeSchema.field(Envelope.FieldName.SOURCE).schema();
            source = createSourceStruct(tableId, sourceSchema);
            LOGGER.warn("Creating snapshot event without OffsetContext - using fallback source struct");
        }

        Instant timestamp = Instant.now();

        // Use EnvelopeSchema.read() to create properly formatted snapshot envelope
        // This ensures schema compatibility between after and envelope
        Struct envelope = tableSchema.getEnvelopeSchema().read(after, source, timestamp);

        // Create SourceRecord (Kafka Connect format)
        SourceRecord sourceRecord = new SourceRecord(
            createSourcePartition(tableId),
            createSourceOffset(),
            tableId.toString(), // topic
            tableSchema.keySchema(),
            key,
            tableSchema.getEnvelopeSchema().schema(),
            envelope
        );

        // Wrap in EmbeddedEngineChangeEvent
        // Constructor signature: (K key, V value, List<Header<H>> headers, SourceRecord sourceRecord)
        return new EmbeddedEngineChangeEvent<>(
            null, // key - not needed for embedded engine
            null, // value - not needed (SourceRecord contains the data)
            null, // headers - not needed
            sourceRecord // the actual SourceRecord with event data
        );
    }

    /**
     * Creates a source struct for snapshot events using the schema from the EnvelopeSchema.
     * This ensures compatibility with the envelope's expected source schema.
     *
     * This is the professional approach: we use the actual source schema from the EnvelopeSchema
     * (which was configured by the PostgreSQL connector) and populate all fields appropriately.
     * Fields that require OffsetContext (LSN, txId) are left null if they're optional.
     */
    private static Struct createSourceStruct(TableId tableId, Schema sourceSchema) {
        Struct source = new Struct(sourceSchema);
        long nowMs = System.currentTimeMillis();

        // Populate all fields that exist in the source schema
        // This handles any PostgreSQL SourceInfo configuration dynamically
        for (org.apache.kafka.connect.data.Field field : sourceSchema.fields()) {
            String fieldName = field.name();

            switch (fieldName) {
                case "version":
                    source.put(fieldName, "3.5.0-SNAPSHOT");
                    break;
                case "connector":
                    source.put(fieldName, "postgresql");
                    break;
                case "name":
                    source.put(fieldName, "snapshot");
                    break;
                case "ts_ms":
                    source.put(fieldName, nowMs);
                    break;
                case "ts_us":
                case "ts_usec":
                    source.put(fieldName, nowMs * 1000);
                    break;
                case "ts_ns":
                    source.put(fieldName, nowMs * 1000000);
                    break;
                case "snapshot":
                    // SnapshotRecord enum values are stored as lowercase strings
                    // See SnapshotRecord.toSource() line 59
                    source.put(fieldName, "true");  // or "incremental" for incremental snapshot
                    break;
                case "last_snapshot_record":
                    source.put(fieldName, false);  // This is a boolean field
                    break;
                case "db":
                    // PostgreSQL SourceInfo convention: use empty string when catalog is null
                    source.put(fieldName, tableId.catalog() != null ? tableId.catalog() : "");
                    break;
                case "schema":
                    // PostgreSQL SourceInfo convention: use empty string when schema is null
                    source.put(fieldName, tableId.schema() != null ? tableId.schema() : "");
                    break;
                case "table":
                    source.put(fieldName, tableId.table());
                    break;
                // LSN and transaction fields - set to null for snapshot (optional fields)
                case "lsn":
                case "lsn_commit":
                case "lsn_proc":
                case "txId":
                case "xmin":
                case "origin":
                case "origin_lsn":
                case "sequence":
                    // These are typically optional and can be null for snapshot events
                    if (field.schema().isOptional()) {
                        source.put(fieldName, null);
                    }
                    break;
                default:
                    // Unknown field - set to null if optional
                    if (field.schema().isOptional()) {
                        source.put(fieldName, null);
                    } else {
                        LOGGER.warn("Unknown required source field '{}' - cannot populate", fieldName);
                    }
            }
        }

        return source;
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
