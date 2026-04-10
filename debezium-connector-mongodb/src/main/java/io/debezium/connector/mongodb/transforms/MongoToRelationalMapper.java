/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.Module;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTime;
import io.debezium.time.NanoDuration;
import io.debezium.time.NanoTime;
import io.debezium.time.Year;
import io.debezium.time.ZonedTime;
import io.debezium.transforms.SmtManager;

/**
 * Converts MongoDB CDC events to relational-style format where 'before' and 'after'
 * fields are nested Struct objects instead of JSON strings. This enables MongoDB events
 * to be processed by relational SMTs like ExtractChangedRecordState.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Divyansh Agrawal
 */
public class MongoToRelationalMapper<R extends ConnectRecord<R>> implements Transformation<R>, Versioned, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoToRelationalMapper.class);

    // Prefix for per-collection schema mapping properties.
    // Usage: schema.mapping.<topic>=fieldName:type,fieldName:type,...
    // Supports primitive types (string, int32, float64, boolean, etc.),
    // logical types (io.debezium.time.Date, org.apache.kafka.connect.data.Decimal), and timestamps.
    private static final String SCHEMA_MAPPING_PREFIX = "schema.mapping.";

    // Default scale for Decimal types when not explicitly specified
    private static final int DEFAULT_DECIMAL_SCALE = 0;

    // Configuration for handling missing fields
    private static final Field ADD_MISSING_FIELDS = Field.create("add.missing.fields")
            .withDisplayName("Add missing fields")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("When true and schema mapping is provided, missing fields will be "
                    + "added with null values to ensure schema consistency.");

    private final Field.Set configFields = Field.setOf(ADD_MISSING_FIELDS);

    private Configuration config;
    private SmtManager<R> smtManager;
    private MongoDataConverter converter;
    private boolean addMissingFields;
    private Map<String, Schema> customSchemaMap = new HashMap<>();

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = Configuration.from(configs);
        this.smtManager = new SmtManager<>(config);

        addMissingFields = config.getBoolean(ADD_MISSING_FIELDS);

        // Per-collection schema mapping strategy:
        // Users define a strict target schema for each collection/topic using individual properties.
        // Example: schema.mapping.myserver.mydb.orders=_id:string,total:float64,created:io.debezium.time.Date
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            if (entry.getKey().startsWith(SCHEMA_MAPPING_PREFIX)) {
                String collectionName = entry.getKey().substring(SCHEMA_MAPPING_PREFIX.length());
                String fieldDefinitions = entry.getValue().toString();
                Schema schema = parseFieldDefinitions(collectionName, fieldDefinitions);
                customSchemaMap.put(collectionName, schema);
            }
        }

        if (!customSchemaMap.isEmpty()) {
            LOGGER.info("Successfully loaded {} custom schema mappings from configuration", customSchemaMap.size());
        }

        // Initialize the MongoDB data converter
        // Using ARRAY encoding to handle nested BSON arrays consistently
        converter = new MongoDataConverter(ExtractNewDocumentState.ArrayEncoding.ARRAY);

        LOGGER.info("MongoToRelationalMapper initialized. Missing fields injection is set to: {}", addMissingFields);
    }

    /**
     * Parses a comma-separated field definition string into a Kafka Connect Schema.
     * Format: "fieldName:type,fieldName:type,..."
     *
     * Supported types:
     * - Primitive: string, int8, int16, int32, int64, float32, float64, boolean, bytes
     * - Logical: io.debezium.time.Date, io.debezium.time.Timestamp, org.apache.kafka.connect.data.Decimal
     */
    private Schema parseFieldDefinitions(String collectionName, String fieldDefinitions) {
        // Initialize an optional Struct schema for the collection's payload
        SchemaBuilder builder = SchemaBuilder.struct().optional().name(collectionName + ".envelope");

        // Iterate through each comma-separated field entry (e.g., "id:int32")
        for (String pair : fieldDefinitions.split(",")) {
            String[] parts = pair.trim().split(":", 2);
            if (parts.length != 2) {
                LOGGER.warn("Skipping malformed field definition '{}' for collection '{}'", pair.trim(), collectionName);
                continue;
            }

            String fieldName = parts[0].trim();
            String fieldType = parts[1].trim();
            // Resolve the type string to a Kafka Connect Schema and add to builder
            builder.field(fieldName, resolveSchema(fieldType));
        }

        return builder.build();
    }

    /**
     * Resolves a type string into a Kafka Connect Schema.
     * Handles both primitive types (by short name) and logical types (by fully-qualified class name).
     */
    private Schema resolveSchema(String fieldType) {
        // Check for logical types first (fully-qualified names)
        switch (fieldType) {
            case "io.debezium.time.Date":
                return io.debezium.time.Date.builder().optional().build();
            case "io.debezium.time.Timestamp":
                return io.debezium.time.Timestamp.builder().optional().build();
            case "io.debezium.time.MicroTimestamp":
                return io.debezium.time.MicroTimestamp.builder().optional().build();
            case "io.debezium.time.NanoTimestamp":
                return io.debezium.time.NanoTimestamp.builder().optional().build();
            case "io.debezium.time.ZonedTimestamp":
                return io.debezium.time.ZonedTimestamp.builder().optional().build();
            case "io.debezium.time.Time":
                return io.debezium.time.Time.builder().optional().build();
            case "io.debezium.time.MicroTime":
                return MicroTime.builder().optional().build();
            case "io.debezium.time.NanoTime":
                return NanoTime.builder().optional().build();
            case "io.debezium.time.ZonedTime":
                return ZonedTime.builder().optional().build();
            case "io.debezium.time.Year":
                return Year.builder().optional().build();
            case "io.debezium.time.MicroDuration":
                return MicroDuration.builder().optional().build();
            case "io.debezium.time.NanoDuration":
                return NanoDuration.builder().optional().build();
            case "io.debezium.data.Json":
                return Json.builder().optional().build();
            case "io.debezium.data.Uuid":
                return Uuid.builder().optional().build();
            case "io.debezium.data.VariableScaleDecimal":
                return VariableScaleDecimal.builder().optional().build();
            case "org.apache.kafka.connect.data.Decimal":
                return Decimal.builder(DEFAULT_DECIMAL_SCALE).optional().build();
            case "org.apache.kafka.connect.data.Timestamp":
                return Timestamp.builder().optional().build();
            case "org.apache.kafka.connect.data.Date":
                return Date.builder().optional().build();
            case "org.apache.kafka.connect.data.Time":
                return Time.builder().optional().build();
            default:
                break;
        }

        // Fall back to primitive type resolution (case-insensitive)
        switch (fieldType.toLowerCase()) {
            case "int8":
                return Schema.OPTIONAL_INT8_SCHEMA;
            case "int16":
                return Schema.OPTIONAL_INT16_SCHEMA;
            case "int32":
            case "integer":
                return Schema.OPTIONAL_INT32_SCHEMA;
            case "int64":
            case "long":
                return Schema.OPTIONAL_INT64_SCHEMA;
            case "float32":
            case "float":
                return Schema.OPTIONAL_FLOAT32_SCHEMA;
            case "float64":
            case "double":
                return Schema.OPTIONAL_FLOAT64_SCHEMA;
            case "boolean":
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case "string":
                return Schema.OPTIONAL_STRING_SCHEMA;
            case "bytes":
                return Schema.OPTIONAL_BYTES_SCHEMA;
            default:
                LOGGER.warn("Unknown type '{}', defaulting to STRING", fieldType);
                return Schema.OPTIONAL_STRING_SCHEMA;
        }
    }

    @Override
    public R apply(R record) {
        // Skip records that don't match the Debezium envelope pattern
        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        Struct value = Requirements.requireStruct(record.value(), "MongoDB envelope");

        // MongoDB uses JSON strings for 'before' and 'after' fields
        String beforeJson = value.getString(Envelope.FieldName.BEFORE);
        String afterJson = value.getString(Envelope.FieldName.AFTER);

        // If both are null, there's nothing for us to convert
        if (beforeJson == null && afterJson == null) {
            return record;
        }

        // Parse the JSON strings into BSON documents
        BsonDocument beforeDoc = beforeJson != null ? BsonDocument.parse(beforeJson) : null;
        BsonDocument afterDoc = afterJson != null ? BsonDocument.parse(afterJson) : null;

        // Relational records expect 'before' and 'after' to share the same schema
        Schema unifiedPayloadSchema = getOrInferPayloadSchema(record, beforeDoc, afterDoc);

        // Convert the BSON documents into Kafka Connect Structs
        Struct beforeStruct = convertToStruct(beforeDoc, unifiedPayloadSchema);
        Struct afterStruct = convertToStruct(afterDoc, unifiedPayloadSchema);

        // Create a new envelope schema that uses nested Structs instead of JSON strings
        Schema envelopeSchema = buildEnvelopeSchema(unifiedPayloadSchema, value.schema());

        // Construct the new envelope value
        Struct envelopeValue = new Struct(envelopeSchema);

        if (beforeStruct != null) {
            envelopeValue.put(Envelope.FieldName.BEFORE, beforeStruct);
        }
        if (afterStruct != null) {
            envelopeValue.put(Envelope.FieldName.AFTER, afterStruct);
        }

        // Replicate the original envelope's metadata
        envelopeValue.put(Envelope.FieldName.OPERATION, value.getString(Envelope.FieldName.OPERATION));
        envelopeValue.put(Envelope.FieldName.SOURCE, value.getStruct(Envelope.FieldName.SOURCE));
        envelopeValue.put(Envelope.FieldName.TIMESTAMP, value.getInt64(Envelope.FieldName.TIMESTAMP));

        if (envelopeSchema.field(Envelope.FieldName.TIMESTAMP_NS) != null) {
            envelopeValue.put(Envelope.FieldName.TIMESTAMP_NS, value.getInt64(Envelope.FieldName.TIMESTAMP_NS));
        }
        if (envelopeSchema.field(Envelope.FieldName.TRANSACTION) != null) {
            envelopeValue.put(Envelope.FieldName.TRANSACTION, value.getStruct(Envelope.FieldName.TRANSACTION));
        }

        // Return a new record with the updated envelope
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                envelopeSchema,
                envelopeValue,
                record.timestamp());
    }

    /**
     * Determines the schema for the 'before' and 'after' fields.
     * Use custom mapping if available, otherwise infer it from the documents.
     */
    private Schema getOrInferPayloadSchema(R record, BsonDocument beforeDoc, BsonDocument afterDoc) {
        // Use explicitly configured schema map
        if (customSchemaMap.containsKey(record.topic())) {
            return customSchemaMap.get(record.topic());
        }

        // Infer schema from the combined field set of before/after
        String schemaName = record.valueSchema().name();
        if (Envelope.isEnvelopeSchema(schemaName)) {
            schemaName = schemaName.substring(0, schemaName.length() - 9); // Remove "Envelope"
        }

        BsonDocument mergedDoc = new BsonDocument();
        if (beforeDoc != null) {
            mergedDoc.putAll(beforeDoc);
        }
        if (afterDoc != null) {
            // 'after' state typically has the most complete/recent field set
            mergedDoc.putAll(afterDoc);
        }

        // Use MongoDataConverter to derive the schema from the merged document
        Map<String, Map<Object, BsonType>> parsedMap = converter.parseBsonDocument(mergedDoc);
        SchemaBuilder builder = SchemaBuilder.struct().name(schemaName).optional();
        converter.buildSchema(parsedMap, builder);

        return builder.build();
    }

    /**
     * Translates a BSON document into a Connect Struct using the target schema.
     */
    private Struct convertToStruct(BsonDocument doc, Schema schema) {
        if (doc == null || schema == null) {
            return null;
        }

        Struct struct = new Struct(schema);

        if (addMissingFields) {
            // Ensure all schema fields are present, even if not in the document
            for (org.apache.kafka.connect.data.Field field : schema.fields()) {
                String fieldName = field.name();
                if (doc.containsKey(fieldName)) {
                    converter.buildStruct(new java.util.AbstractMap.SimpleEntry<>(fieldName, doc.get(fieldName)), schema, struct);
                }
                else {
                    struct.put(fieldName, null);
                }
            }
        }
        else {
            // Only process fields that exist in the document
            for (Map.Entry<String, BsonValue> entry : doc.entrySet()) {
                if (schema.field(entry.getKey()) != null) {
                    converter.buildStruct(entry, schema, struct);
                }
            }
        }

        return struct;
    }

    /**
     * Constructs a relational-style envelope schema by replacing 'before' and 'after'
     * string fields with nested struct fields.
     */
    private Schema buildEnvelopeSchema(Schema payloadSchema, Schema originalEnvelopeSchema) {
        SchemaBuilder builder = SchemaBuilder.struct()
                .name(originalEnvelopeSchema.name())
                .version(originalEnvelopeSchema.version());

        for (org.apache.kafka.connect.data.Field field : originalEnvelopeSchema.fields()) {
            if (Envelope.FieldName.BEFORE.equals(field.name()) || Envelope.FieldName.AFTER.equals(field.name())) {
                // Swap string schema with our nested struct schema
                builder.field(field.name(), payloadSchema);
            }
            else {
                // Keep all other fields (source, op, ts_ms, etc.) the same
                builder.field(field.name(), field.schema());
            }
        }

        return builder.build();
    }

    @Override
    public Field.Set getConfigFields() {
        return configFields;
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, configFields.asArray());
        return config;
    }

    @Override
    public void close() {
        // No persistent resources to release
    }

    @Override
    public String version() {
        return Module.version();
    }
}
