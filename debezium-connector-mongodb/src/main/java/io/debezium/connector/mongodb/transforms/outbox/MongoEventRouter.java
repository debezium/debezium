/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms.outbox;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Transformation;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState;
import io.debezium.connector.mongodb.transforms.MongoDataConverter;
import io.debezium.data.Envelope;
import io.debezium.time.Timestamp;
import io.debezium.transforms.outbox.EventRouter;
import io.debezium.transforms.outbox.EventRouterConfigDefinition;

/**
 * Debezium MongoDB Outbox Event Router SMT
 *
 * @author Sungho Hwang
 */
public class MongoEventRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger logger = LoggerFactory.getLogger(MongoEventRouter.class);

    private final JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
            .outputMode(JsonMode.EXTENDED)
            .indent(true)
            .newLineCharacters("\n")
            .build();

    private final MongoDataConverter converter = new MongoDataConverter(ExtractNewDocumentState.ArrayEncoding.ARRAY);

    private String fieldTimestamp;
    private String fieldPayload;
    private boolean expandPayload;

    private final ExtractField<R> afterExtractor = new ExtractField.Value<>();
    private final EventRouter<R> sqlOutboxEventRouter = new EventRouter<>();

    @Override
    public R apply(R r) {
        R expandedAfterFieldRecord = r;
        try {
            expandedAfterFieldRecord = expandAfterField(r);
        }
        catch (Exception e) {
            // when the record includes update event or
            // 'after' field is not valid MongoDB data format for converting to Kafka Connect data format
            logger.warn("Filed to expand after field: " + e.getMessage(), e);
        }

        return sqlOutboxEventRouter.apply(expandedAfterFieldRecord);
    }

    @Override
    public ConfigDef config() {
        return MongoEventRouterConfigDefinition.configDef();
    }

    @Override
    public void close() {
        sqlOutboxEventRouter.close();
    }

    @Override
    public void configure(Map<String, ?> configMap) {
        final Configuration config = Configuration.from(configMap);
        fieldTimestamp = config.getString(MongoEventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP);
        expandPayload = config.getBoolean(MongoEventRouterConfigDefinition.EXPAND_JSON_PAYLOAD);
        fieldPayload = config.getString(MongoEventRouterConfigDefinition.FIELD_PAYLOAD);

        final Map<String, String> afterExtractorConfig = new HashMap<>();
        afterExtractorConfig.put("field", Envelope.FieldName.AFTER);

        afterExtractor.configure(afterExtractorConfig);

        // Convert configuration fields from MongoDB Outbox Event Router to SQL Outbox Event Router's
        Map<String, ?> convertedConfigMap = convertConfigMap(configMap);

        sqlOutboxEventRouter.configure(convertedConfigMap);
    }

    private R expandAfterField(R originalRecord) throws Exception {
        final R afterRecord = afterExtractor.apply(originalRecord);

        // Convert 'after' field format from JSON String to Struct
        Object after = afterRecord.value();

        // For delete operation
        if (after == null) {
            return originalRecord.newRecord(
                    originalRecord.topic(),
                    originalRecord.kafkaPartition(),
                    originalRecord.keySchema(),
                    originalRecord.key(),
                    SchemaBuilder.struct().build(),
                    null,
                    originalRecord.timestamp(),
                    originalRecord.headers());
        }

        if (!(after instanceof String)) {
            throw new Exception("Unable to expand non-String after field: " + after.getClass());
        }

        Schema originalValueSchema = originalRecord.valueSchema();

        String afterSchemaName = afterRecord.valueSchema().name();
        BsonDocument afterBsonDocument = BsonDocument.parse((String) after);

        Schema newAfterSchema = buildNewAfterSchema(afterSchemaName, afterBsonDocument);
        Struct newAfterStruct = buildNewAfterStruct(newAfterSchema, afterBsonDocument);

        String valueSchemaName = originalValueSchema.name();

        Schema newValueSchema = buildNewValueSchema(valueSchemaName, originalValueSchema, newAfterSchema);
        Struct newValueStruct = buildNewValueStruct((Struct) originalRecord.value(), newValueSchema, newAfterStruct);

        return originalRecord.newRecord(
                originalRecord.topic(),
                originalRecord.kafkaPartition(),
                originalRecord.keySchema(),
                originalRecord.key(),
                newValueSchema,
                newValueStruct,
                originalRecord.timestamp(),
                originalRecord.headers());
    }

    private Schema buildNewAfterSchema(String schemaName, BsonDocument afterBsonDocument) {
        SchemaBuilder afterSchemaBuilder = SchemaBuilder.struct().name(schemaName);

        for (Map.Entry<String, BsonValue> entry : afterBsonDocument.entrySet()) {
            String entryKey = entry.getKey();

            if (entryKey.equals(fieldTimestamp)) {
                afterSchemaBuilder.field(fieldTimestamp, Timestamp.schema());
            }
            else if (entryKey.equals(fieldPayload)
                    && !expandPayload
                    && entry.getValue() instanceof BsonDocument) {
                afterSchemaBuilder.field(fieldPayload, Schema.OPTIONAL_STRING_SCHEMA);
            }
            else {
                converter.addFieldSchema(entry, afterSchemaBuilder);
            }
        }

        return afterSchemaBuilder.build();
    }

    private Struct buildNewAfterStruct(Schema afterSchema, BsonDocument afterBsonDocument) {
        Struct afterStruct = new Struct(afterSchema);

        for (Map.Entry<String, BsonValue> entry : afterBsonDocument.entrySet()) {
            String entryKey = entry.getKey();

            if (entryKey.equals(fieldTimestamp)) {
                afterStruct.put(fieldTimestamp, entry.getValue().asInt64().getValue());
            }
            else if (entryKey.equals(fieldPayload)
                    && !expandPayload
                    && entry.getValue() instanceof BsonDocument) {
                afterStruct.put(fieldPayload, entry.getValue().asDocument().toJson(jsonWriterSettings));
            }
            else {
                converter.convertRecord(entry, afterSchema, afterStruct);
            }
        }

        return afterStruct;
    }

    private Schema buildNewValueSchema(String valueSchemaName, Schema originalValueSchema, Schema afterSchema) {
        SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().name(valueSchemaName);
        for (Field field : originalValueSchema.fields()) {
            if (field.name().equals("after")) {
                continue;
            }
            valueSchemaBuilder.field(field.name(), field.schema());
        }

        valueSchemaBuilder.field("after", afterSchema);

        return valueSchemaBuilder.build();
    }

    private Struct buildNewValueStruct(Struct originalValueStruct, Schema newValueSchema, Struct newAfterStruct) {
        Struct newValueStruct = new Struct(newValueSchema);
        for (Field field : originalValueStruct.schema().fields()) {
            if (field.name().equals("after")) {
                continue;
            }
            newValueStruct.put(field.name(), originalValueStruct.get(field));
        }

        newValueStruct.put("after", newAfterStruct);

        return newValueStruct;
    }

    private <T> Map<String, T> convertConfigMap(Map<String, T> oldConfigMap) {
        Map<String, String> fieldNameConverter = createFieldNameConverter();

        Map<String, T> newConfigMap = new HashMap<>();
        for (String config : oldConfigMap.keySet()) {
            if (fieldNameConverter.containsKey(config)) {
                newConfigMap.put(fieldNameConverter.get(config), oldConfigMap.get(config));
            }
        }

        // To convert default event id field name when the configuration is not set
        if (!hasConfigFieldEventId(newConfigMap)) {
            newConfigMap.put(
                    EventRouterConfigDefinition.FIELD_EVENT_ID.name(),
                    (T) MongoEventRouterConfigDefinition.FIELD_EVENT_ID.defaultValue());
        }

        return newConfigMap;
    }

    private <T> boolean hasConfigFieldEventId(Map<String, T> configMap) {
        return configMap.containsKey(EventRouterConfigDefinition.FIELD_EVENT_ID.name());
    }

    private Map<String, String> createFieldNameConverter() {
        Map<String, String> fieldNameConverter = new HashMap<>();

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.FIELD_EVENT_ID.name(),
                EventRouterConfigDefinition.FIELD_EVENT_ID.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.FIELD_EVENT_KEY.name(),
                EventRouterConfigDefinition.FIELD_EVENT_KEY.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.FIELD_EVENT_TYPE.name(),
                EventRouterConfigDefinition.FIELD_EVENT_TYPE.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(),
                EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.FIELD_PAYLOAD.name(),
                EventRouterConfigDefinition.FIELD_PAYLOAD.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.FIELD_PAYLOAD_ID.name(),
                EventRouterConfigDefinition.FIELD_PAYLOAD_ID.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(),
                EventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.ROUTE_BY_FIELD.name(),
                EventRouterConfigDefinition.ROUTE_BY_FIELD.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.ROUTE_TOPIC_REGEX.name(),
                EventRouterConfigDefinition.ROUTE_TOPIC_REGEX.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.ROUTE_TOPIC_REPLACEMENT.name(),
                EventRouterConfigDefinition.ROUTE_TOPIC_REPLACEMENT.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD.name(),
                EventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD.name());

        fieldNameConverter.put(
                MongoEventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR.name(),
                EventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR.name());

        return fieldNameConverter;
    }
}
