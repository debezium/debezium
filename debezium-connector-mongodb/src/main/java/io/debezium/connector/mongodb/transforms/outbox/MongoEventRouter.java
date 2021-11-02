/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState;
import io.debezium.connector.mongodb.transforms.MongoDataConverter;
import io.debezium.data.Envelope;
import io.debezium.time.Timestamp;
import io.debezium.transforms.outbox.EventRouter;
import io.debezium.transforms.outbox.EventRouterConfigDefinition;
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

import java.util.HashMap;
import java.util.Map;

/**
 * Debezium MongoDB Outbox Event Router SMT
 *
 * @author Sungho Hwang
 */
public class MongoEventRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoEventRouter.class);
    private final JsonWriterSettings COMPACT_JSON_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.EXTENDED)
            .indent(true)
            .indentCharacters("")
            .newLineCharacters("")
            .build();
    private final MongoDataConverter converter = new MongoDataConverter(ExtractNewDocumentState.ArrayEncoding.ARRAY);

    private final ObjectMapper objectMapper = new ObjectMapper();

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
            LOGGER.warn("Filed to expand after field: " + e.getMessage(), e);
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

        Map<String, ?> convertedConfigMap = convertConfigMap(configMap);

        sqlOutboxEventRouter.configure(convertedConfigMap);
    }

    private R expandAfterField(R originalRecord) throws Exception {
        final R afterRecord = afterExtractor.apply(originalRecord);

        // Convert 'after' field format from JSON String to Struct
        Object after = afterRecord.value();

        // Operation is 'Delete'
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

        SchemaBuilder afterSchemaBuilder = SchemaBuilder.struct().name(afterRecord.valueSchema().name());

        String afterString = (String) after;

        BsonDocument afterBsonDocument = BsonDocument.parse(afterString);
        for (Map.Entry<String, BsonValue> entry : afterBsonDocument.entrySet()) {
            if (entry.getKey().equals(fieldTimestamp)) {
                afterSchemaBuilder.field(fieldTimestamp, Timestamp.schema());
            }
            else if (entry.getKey().equals(fieldPayload)
                    && !expandPayload
                    && entry.getValue() instanceof BsonDocument) {
                afterSchemaBuilder.field(fieldPayload, Schema.OPTIONAL_STRING_SCHEMA);
            }
            else {
                converter.addFieldSchema(entry, afterSchemaBuilder);
            }
        }

        Schema afterSchema = afterSchemaBuilder.build();
        Struct afterStruct = new Struct(afterSchema);

        for (Map.Entry<String, BsonValue> entry : afterBsonDocument.entrySet()) {
            String entryKey = entry.getKey();

            if (entryKey.equals(fieldTimestamp)) {
                afterStruct.put(fieldTimestamp, entry.getValue().asInt64().getValue());
            }
            else if (entryKey.equals(fieldPayload) && !expandPayload && entry.getValue() instanceof BsonDocument) {
                afterStruct.put(fieldPayload, entry.getValue().asDocument().toJson(COMPACT_JSON_SETTINGS));
            }
            else {
                converter.convertRecord(entry, afterSchema, afterStruct);
            }
        }

        SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().name(originalRecord.valueSchema().name());
        for (Field field : originalRecord.valueSchema().fields()) {
            if (field.name().equals("after"))
                continue;
            valueSchemaBuilder.field(field.name(), field.schema());
        }
        valueSchemaBuilder.field("after", afterSchema);
        Schema valueSchema = valueSchemaBuilder.build();
        Struct valueStruct = new Struct(valueSchema);

        for (Field field : originalRecord.valueSchema().fields()) {
            if (field.name().equals("after"))
                continue;
            valueStruct.put(field.name(), ((Struct) originalRecord.value()).get(field));
        }

        valueStruct.put("after", afterStruct);

        return originalRecord.newRecord(
                originalRecord.topic(),
                originalRecord.kafkaPartition(),
                originalRecord.keySchema(),
                originalRecord.key(),
                valueStruct.schema(),
                valueStruct,
                originalRecord.timestamp(),
                originalRecord.headers());
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
        fieldNameConverter.put(MongoEventRouterConfigDefinition.FIELD_EVENT_ID.name(), EventRouterConfigDefinition.FIELD_EVENT_ID.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.FIELD_EVENT_KEY.name(), EventRouterConfigDefinition.FIELD_EVENT_KEY.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.FIELD_EVENT_TYPE.name(), EventRouterConfigDefinition.FIELD_EVENT_TYPE.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.FIELD_PAYLOAD.name(), EventRouterConfigDefinition.FIELD_PAYLOAD.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.FIELD_PAYLOAD_ID.name(), EventRouterConfigDefinition.FIELD_PAYLOAD_ID.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), EventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.ROUTE_BY_FIELD.name(), EventRouterConfigDefinition.ROUTE_BY_FIELD.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.ROUTE_TOPIC_REGEX.name(), EventRouterConfigDefinition.ROUTE_TOPIC_REGEX.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.ROUTE_TOPIC_REPLACEMENT.name(), EventRouterConfigDefinition.ROUTE_TOPIC_REPLACEMENT.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD.name(),
                EventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD.name());
        fieldNameConverter.put(MongoEventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR.name(), EventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR.name());

        return fieldNameConverter;
    }
}
