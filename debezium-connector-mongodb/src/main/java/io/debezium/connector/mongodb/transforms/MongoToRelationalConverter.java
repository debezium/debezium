/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.Module;
import io.debezium.transforms.SmtManager;

/**
 * Converts MongoDB CDC events to relational-style format where 'before' and 'after'
 * fields are nested Struct objects instead of JSON strings. This enables MongoDB events
 * to be processed by relational SMTs like ExtractChangedRecordState.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Divyansh Agrawal
 */
public class MongoToRelationalConverter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoToRelationalConverter.class);

    // Configuration field for schema mapping
    private static final Field SCHEMA_MAPPING = Field.create("schema.mapping")
            .withDisplayName("Schema mapping configuration")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault("")
            .withDescription("JSON configuration for mapping MongoDB documents to a target schema. "
                    + "When provided, the SMT will normalize documents to match this schema, "
                    + "adding null values for missing fields.");

    // Configuration for handling missing fields
    private static final Field ADD_MISSING_FIELDS = Field.create("add.missing.fields")
            .withDisplayName("Add missing fields")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("When true and schema mapping is provided, missing fields will be "
                    + "added with null values to ensure schema consistency.");

    private final Field.Set configFields = Field.setOf(SCHEMA_MAPPING, ADD_MISSING_FIELDS);

    private Configuration config;
    private SmtManager<R> smtManager;
    private MongoDataConverter converter;
    private boolean addMissingFields;
    private String schemaMappingJson; 

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = Configuration.from(configs);
        this.smtManager = new SmtManager<>(config);

        addMissingFields = config.getBoolean(ADD_MISSING_FIELDS);
        schemaMappingJson = config.getString(SCHEMA_MAPPING);

        // Initialize the MongoDB data converter
        // Using ARRAY encoding and default field naming
        converter = new MongoDataConverter(ExtractNewDocumentState.ArrayEncoding.ARRAY);

        LOGGER.info("MongoToRelationalConverter configured with addMissingFields={}", addMissingFields);
    }

    @Override
    public R apply(R record) {
        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        Struct value = Requirements.requireStruct(record.value(), "MongoDB envelope");
        
        String beforeJson = value.getString(Envelope.FieldName.BEFORE);
        String afterJson = value.getString(Envelope.FieldName.AFTER);
        
        // If there's no payload string to parse, we can't do the conversion
        if (beforeJson == null && afterJson == null) {
            return record;
        }

        BsonDocument beforeDoc = beforeJson != null ? BsonDocument.parse(beforeJson) : null;
        BsonDocument afterDoc = afterJson != null ? BsonDocument.parse(afterJson) : null;
        
        // Relational envelopes require the before and after payloads to share the exact same schema.
        // Therefore, we must infer a unified schema that covers all fields present in either document.
        Schema unifiedPayloadSchema = getOrInferPayloadSchema(record, beforeDoc, afterDoc);
        
        Struct beforeStruct = convertToStruct(beforeDoc, unifiedPayloadSchema);
        Struct afterStruct = convertToStruct(afterDoc, unifiedPayloadSchema);
        
        // Build the relational-style envelope schema
        Schema envelopeSchema = buildEnvelopeSchema(unifiedPayloadSchema, value.schema());
        
        // Create the new envelope value with nested Structs
        Struct envelopeValue = new Struct(envelopeSchema);
        
        if (beforeStruct != null) {
            envelopeValue.put(Envelope.FieldName.BEFORE, beforeStruct);
        }
        if (afterStruct != null) {
            envelopeValue.put(Envelope.FieldName.AFTER, afterStruct);
        }
        
        // Copy standard Debezium envelope fields
        envelopeValue.put(Envelope.FieldName.OPERATION, value.getString(Envelope.FieldName.OPERATION));
        envelopeValue.put(Envelope.FieldName.SOURCE, value.getStruct(Envelope.FieldName.SOURCE));
        envelopeValue.put(Envelope.FieldName.TIMESTAMP, value.getInt64(Envelope.FieldName.TIMESTAMP));
        
        if (envelopeSchema.field(Envelope.FieldName.TIMESTAMP_NS) != null) {
            envelopeValue.put(Envelope.FieldName.TIMESTAMP_NS, value.getInt64(Envelope.FieldName.TIMESTAMP_NS));
        }
        if (envelopeSchema.field(Envelope.FieldName.TRANSACTION) != null) {
            envelopeValue.put(Envelope.FieldName.TRANSACTION, value.getStruct(Envelope.FieldName.TRANSACTION));
        }
        
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
     * Infers a single unified schema combining fields from both before and after states.
     */
    private Schema getOrInferPayloadSchema(R record, BsonDocument beforeDoc, BsonDocument afterDoc) {
        String schemaName = record.valueSchema().name();
        if (Envelope.isEnvelopeSchema(schemaName)) {
            schemaName = schemaName.substring(0, schemaName.length() - 9); // Remove "Envelope"
        }

        
        BsonDocument mergedDoc = new BsonDocument();
        if (beforeDoc != null) {
            mergedDoc.putAll(beforeDoc);
        }
        if (afterDoc != null) {
            // after fields naturally overwrite before fields for schema determination
            mergedDoc.putAll(afterDoc); 
        }

        Map<String, Map<Object, BsonType>> parsedMap = converter.parseBsonDocument(mergedDoc);
        SchemaBuilder builder = SchemaBuilder.struct().name(schemaName).optional();
        converter.buildSchema(parsedMap, builder);
        
        return builder.build();
    }

    /**
     * Converts a BsonDocument into a Kafka Connect Struct based on the defined schema.
     */
    private Struct convertToStruct(BsonDocument doc, Schema schema) {
        if (doc == null || schema == null) {
            return null;
        }
        
        Struct struct = new Struct(schema);
        for (Map.Entry<String, BsonValue> entry : doc.entrySet()) {
            converter.buildStruct(entry, schema, struct);
        }
        return struct;
    }

    /**
     * Reconstructs the Debezium Envelope Schema, replacing 'before' and 'after' String schemas 
     * with the fully nested Relational Payload Schema.
     */
    private Schema buildEnvelopeSchema(Schema payloadSchema, Schema originalEnvelopeSchema) {
        SchemaBuilder builder = SchemaBuilder.struct()
                .name(originalEnvelopeSchema.name())
                .version(originalEnvelopeSchema.version());

        for (org.apache.kafka.connect.data.Field field : originalEnvelopeSchema.fields()) {
            if (Envelope.FieldName.BEFORE.equals(field.name()) || Envelope.FieldName.AFTER.equals(field.name())) {
                // Ensure before and after use the exact same nested Struct schema (payloadSchema)
                builder.field(field.name(), payloadSchema);
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        return builder.build();
    }

    public Iterable<Field> validateConfigFields() {
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
    }

    @Override
    public String version() {
        return Module.version();
    }
}
