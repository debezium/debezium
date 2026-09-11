/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
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
import io.debezium.data.SchemaUtil;
import io.debezium.metadata.ConfigDescriptor;
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

    private static final String SCHEMA_MAPPING_PREFIX = "schema.mapping.";

    // Configuration for handling missing fields
    private static final Field ADD_MISSING_FIELDS = Field.create("add.missing.fields")
            .withDisplayName("Add missing fields")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("Explicitly assigns null to absent fields when inferring a document schema. "
                    + "Configured schema mappings always retain every optional output field and use null for unresolved paths.");

    private Field.Set configFields = Field.setOf(ADD_MISSING_FIELDS);

    private Configuration config;
    private SmtManager<R> smtManager;
    private MongoDataConverter converter;
    private boolean addMissingFields;
    private final Map<String, MongoDocumentMapping> customSchemaMap = new HashMap<>();

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = Configuration.from(configs);
        this.smtManager = new SmtManager<>(config);

        addMissingFields = config.getBoolean(ADD_MISSING_FIELDS);

        smtManager.validate(config, Field.setOf(ADD_MISSING_FIELDS));
        customSchemaMap.clear();
        configFields = Field.setOf(ADD_MISSING_FIELDS);
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            if (entry.getKey().startsWith(SCHEMA_MAPPING_PREFIX)) {
                final String namespace = entry.getKey().substring(SCHEMA_MAPPING_PREFIX.length());
                final int separator = namespace.indexOf('.');
                if (separator <= 0 || separator == namespace.length() - 1 || !namespace.equals(namespace.trim())) {
                    throw new ConfigException("Schema mapping keys must identify a database and collection: " + entry.getKey());
                }
                if (!(entry.getValue() instanceof String json)) {
                    throw new ConfigException("Schema mapping values must be JSON strings: " + entry.getKey());
                }
                customSchemaMap.put(namespace, new MongoDocumentMapping(namespace, json));
                configFields = configFields.with(Field.create(entry.getKey())
                        .withType(ConfigDef.Type.STRING)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("JSON object mapping output fields to BSON document JSON Pointers and Connect types"));
            }
        }

        // Initialize the MongoDB data converter
        // Using ARRAY encoding to handle nested BSON arrays consistently
        converter = new MongoDataConverter(ExtractNewDocumentState.ArrayEncoding.ARRAY);

        LOGGER.info("MongoToRelationalMapper initialized. Missing fields injection is set to: {}", addMissingFields);
    }

    @Override
    public R apply(R record) {
        if (record == null || record.value() == null || !smtManager.isValidEnvelope(record)) {
            return record;
        }
        final var value = Requirements.requireStruct(record.value(), "MongoDB envelope");
        for (String field : List.of(Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER)) {
            if (value.schema().field(field) == null || value.schema().field(field).schema().type() != Schema.Type.STRING) {
                return record;
            }
        }
        final String beforeJson = value.getString(Envelope.FieldName.BEFORE);
        final String afterJson = value.getString(Envelope.FieldName.AFTER);
        final var operation = Envelope.Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
        if (afterJson == null && (operation == Envelope.Operation.UPDATE || operation == Envelope.Operation.CREATE || operation == Envelope.Operation.READ)) {
            throw new DataException("MongoToRelationalMapper requires a full after document for create, snapshot, and update events. "
                    + "Use a change_streams_update_full capture mode and ensure that the document image is available.");
        }
        final var beforeDoc = beforeJson != null ? BsonDocument.parse(beforeJson) : null;
        final var afterDoc = afterJson != null ? BsonDocument.parse(afterJson) : null;
        final var mapping = mappingFor(value);
        final var payloadSchema = mapping != null ? mapping.schema() : getOrInferPayloadSchema(record, beforeDoc, afterDoc);
        final var before = mapping != null ? mapping.convert(beforeDoc, beforeJson) : convertToStruct(beforeDoc, payloadSchema);
        final var after = mapping != null ? mapping.convert(afterDoc, afterJson) : convertToStruct(afterDoc, payloadSchema);
        final var envelopeSchema = buildEnvelopeSchema(payloadSchema, value.schema());
        final var envelope = new Struct(envelopeSchema);
        // Copy every envelope field so custom metadata and future additions survive the transformation.
        for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
            envelope.put(field.name(), switch (field.name()) {
                case Envelope.FieldName.BEFORE -> before;
                case Envelope.FieldName.AFTER -> after;
                default -> value.getWithoutDefault(field.name());
            });
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                envelopeSchema, envelope, record.timestamp(), record.headers());
    }

    private MongoDocumentMapping mappingFor(Struct envelope) {
        if (customSchemaMap.isEmpty()) {
            return null;
        }
        final var source = envelope.getStruct(Envelope.FieldName.SOURCE);
        if (source == null || source.schema().field("db") == null || source.schema().field("collection") == null) {
            throw new DataException("Collection schema mappings require source.db and source.collection metadata");
        }
        return customSchemaMap.get(source.getString("db") + "." + source.getString("collection"));
    }

    /**
     * Determines the schema for the 'before' and 'after' fields.
     * Use custom mapping if available, otherwise infer it from the documents.
     */
    private Schema getOrInferPayloadSchema(R record, BsonDocument beforeDoc, BsonDocument afterDoc) {
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
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(originalEnvelopeSchema);

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
