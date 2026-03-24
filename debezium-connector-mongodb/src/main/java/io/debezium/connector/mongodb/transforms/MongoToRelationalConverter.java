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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.Module;
import io.debezium.transforms.SmtManager;
import io.debezium.util.Strings;

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
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
    private Map<String, Schema> customSchemaMap = new HashMap<>();

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = Configuration.from(configs);
        this.smtManager = new SmtManager<>(config);

        addMissingFields = config.getBoolean(ADD_MISSING_FIELDS);
        schemaMappingJson = config.getString(SCHEMA_MAPPING);

        // Implementation of the Schema Mapping Strategy:
        // MongoDB's dynamic schemas can cause missing field scenarios.
        // This strategy allows users to define a strict target schema per collection via JSON configuration.
        // If provided, we pre-compile these mappings into standard Kafka Connect Schemas at startup.
        if (!Strings.isNullOrBlank(schemaMappingJson)) {
            try {
                JsonNode rootNode = MAPPER.readTree(schemaMappingJson);
                
                // Iterate through the JSON definition, where keys are the expected collection names
                rootNode.fields().forEachRemaining(collectionEntry -> {
                    String collectionName = collectionEntry.getKey();
                    JsonNode fieldsNode = collectionEntry.getValue();
                    
                    SchemaBuilder builder = SchemaBuilder.struct().optional().name(collectionName + ".envelope");
                    
                    // Translate the user's string type definitions into actual Connect Schema types
                    fieldsNode.fields().forEachRemaining(fieldEntry -> {
                        String fieldName = fieldEntry.getKey();
                        String fieldType = fieldEntry.getValue().asText().toLowerCase();
                        
                        switch (fieldType) {
                            case "int8":     builder.field(fieldName, Schema.OPTIONAL_INT8_SCHEMA); break;
                            case "int16":    builder.field(fieldName, Schema.OPTIONAL_INT16_SCHEMA); break;
                            case "int32":
                            case "integer":  builder.field(fieldName, Schema.OPTIONAL_INT32_SCHEMA); break;
                            case "int64":
                            case "long":     builder.field(fieldName, Schema.OPTIONAL_INT64_SCHEMA); break;
                            case "float32":
                            case "float":    builder.field(fieldName, Schema.OPTIONAL_FLOAT32_SCHEMA); break;
                            case "float64":
                            case "double":   builder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA); break;
                            case "boolean":  builder.field(fieldName, Schema.OPTIONAL_BOOLEAN_SCHEMA); break;
                            case "string":   builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA); break;
                            case "bytes":    builder.field(fieldName, Schema.OPTIONAL_BYTES_SCHEMA); break;
                            default:         builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
                        }
                    });
                    
                    // Cache the compiled schema so we can instantly apply it to incoming records
                    customSchemaMap.put(collectionName, builder.build());
                });
                LOGGER.info("Successfully loaded {} custom schema mappings from configuration", customSchemaMap.size());
            } catch (Exception e) {
                LOGGER.error("Failed to parse the schema.mapping JSON configuration", e);
            }
        }

        // Initialize the MongoDB data converter
        // Using ARRAY encoding to handle nested BSON arrays consistently
        converter = new MongoDataConverter(ExtractNewDocumentState.ArrayEncoding.ARRAY);

        LOGGER.info("MongoToRelationalConverter initialized. Missing fields injection is set to: {}", addMissingFields);
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
        //Use explicitly configured schema map
        if (customSchemaMap.containsKey(record.topic())) {
            return customSchemaMap.get(record.topic());
        }

        //Infer schema from the combined field set of before/after
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
                } else {
                    struct.put(fieldName, null);
                }
            }
        } else {
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
            } else {
                // Keep all other fields (source, op, ts_ms, etc.) the same
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
        // No persistent resources to release
    }

    @Override
    public String version() {
        return Module.version();
    }
}

