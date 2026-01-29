/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms;

import static io.debezium.connector.postgresql.LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY;
import static io.debezium.connector.postgresql.LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY;
import static io.debezium.connector.postgresql.PostgresSchemaFactory.POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_VALUE_SCHEMA_NAME;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.ReplaceField;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.schema.FieldNameSelector;
import io.debezium.transforms.ConnectRecordUtil;
import io.debezium.transforms.outbox.EventRouterConfigDefinition;
import io.debezium.transforms.outbox.JsonSchemaData;
import io.debezium.util.BoundedConcurrentHashMap;

/**
 * The transform converts binary content of a logical decoding message to a structured form.
 * One of the possible usages is to apply the transform before {@link io.debezium.transforms.outbox.EventRouter} so that the transform
 * will produce a record suitable for the Outbox SMT.
 *
 * @author Roman Kudryashov
 */
public class DecodeLogicalDecodingMessageContent<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(DecodeLogicalDecodingMessageContent.class);

    public static final io.debezium.config.Field FIELDS_NULL_INCLUDE = io.debezium.config.Field.create("fields.null.include")
            .withDisplayName("Defines whether to include fields with null values to the decoded structure")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Defines whether to include fields with null values to the decoded structure");

    private ObjectMapper objectMapper;
    private JsonSchemaData jsonSchemaData;
    private BoundedConcurrentHashMap<Schema, Schema> logicalDecodingMessageContentSchemaCache;

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        io.debezium.config.Field.group(config, null, FIELDS_NULL_INCLUDE);
        return config;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);

        objectMapper = new ObjectMapper();

        boolean fieldsNullInclude = config.getBoolean(FIELDS_NULL_INCLUDE);
        EventRouterConfigDefinition.JsonPayloadNullFieldBehavior nullFieldBehavior = fieldsNullInclude
                ? EventRouterConfigDefinition.JsonPayloadNullFieldBehavior.OPTIONAL_BYTES
                : EventRouterConfigDefinition.JsonPayloadNullFieldBehavior.IGNORE;
        jsonSchemaData = new JsonSchemaData(nullFieldBehavior,
                FieldNameSelector.defaultNonRelationalSelector(CommonConnectorConfig.FieldNameAdjustmentMode.NONE.createAdjuster()));

        logicalDecodingMessageContentSchemaCache = new BoundedConcurrentHashMap<>(10000, 10, BoundedConcurrentHashMap.Eviction.LRU);
    }

    @Override
    public R apply(final R record) {
        // ignore all messages that are not logical decoding messages
        if (!Objects.equals(record.valueSchema().name(), POSTGRES_LOGICAL_DECODING_MESSAGE_MONITOR_VALUE_SCHEMA_NAME)) {
            LOGGER.debug("Ignore not a logical decoding message. Message key: \"{}\"", record.key());
            return record;
        }

        Struct originalValue = requireStruct(record.value(), "Retrieve a record value");
        Struct logicalDecodingMessageContent = getLogicalDecodingMessageContent(originalValue);
        R recordWithoutMessageField = removeLogicalDecodingMessageContentField(record);

        final Schema updatedValueSchema = getUpdatedValueSchema(logicalDecodingMessageContent.schema(), recordWithoutMessageField.valueSchema());
        final Struct updatedValue = getUpdatedValue(updatedValueSchema, originalValue, logicalDecodingMessageContent);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                null,
                // clear prefix of a logical decoding message
                null,
                updatedValueSchema,
                updatedValue,
                record.timestamp(),
                record.headers());
    }

    private Struct getLogicalDecodingMessageContent(Struct valueStruct) {
        Struct logicalDecodingMessageStruct = requireStruct(valueStruct.get(DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY),
                "Retrieve content of a logical decoding message");

        if (logicalDecodingMessageStruct.schema().field(DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY).schema().type() != Schema.Type.BYTES) {
            throw new DebeziumException("The content of a logical decoding message is non-binary");
        }

        byte[] logicalDecodingMessageContentBytes = logicalDecodingMessageStruct.getBytes(DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY);
        return convertLogicalDecodingMessageContentBytesToStruct(logicalDecodingMessageContentBytes);
    }

    private Struct convertLogicalDecodingMessageContentBytesToStruct(byte[] logicalDecodingMessageContent) {
        final String logicalDecodingMessageContentString = new String(logicalDecodingMessageContent);
        // parse and get Jackson JsonNode
        final JsonNode logicalDecodingMessageContentJson = parseLogicalDecodingMessageContentJsonString(logicalDecodingMessageContentString);
        // get schema of a logical decoding message content
        Schema logicalDecodingMessageContentSchema = jsonSchemaData.toConnectSchema(null, logicalDecodingMessageContentJson);
        // get Struct of a logical decoding message content
        return (Struct) jsonSchemaData.toConnectData(logicalDecodingMessageContentJson, logicalDecodingMessageContentSchema);
    }

    private JsonNode parseLogicalDecodingMessageContentJsonString(String logicalDecodingMessageContentJsonString) {
        if (logicalDecodingMessageContentJsonString.startsWith("{") || logicalDecodingMessageContentJsonString.startsWith("[")) {
            try {
                return objectMapper.readTree(logicalDecodingMessageContentJsonString);
            }
            catch (JsonProcessingException e) {
                throw new DebeziumException(e);
            }
        }
        throw new DebeziumException(
                "Unable to parse logical decoding message content JSON string '" + logicalDecodingMessageContentJsonString + "'");
    }

    private R removeLogicalDecodingMessageContentField(R record) {
        final ReplaceField<R> dropFieldDelegate = ConnectRecordUtil.dropFieldFromValueDelegate(DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY);
        return dropFieldDelegate.apply(record);
    }

    private Schema getUpdatedValueSchema(Schema logicalDecodingMessageContentSchema, Schema debeziumEventSchema) {
        Schema valueSchema = logicalDecodingMessageContentSchemaCache.get(logicalDecodingMessageContentSchema);
        if (valueSchema == null) {
            valueSchema = getSchemaBuilder(logicalDecodingMessageContentSchema, debeziumEventSchema).build();
            logicalDecodingMessageContentSchemaCache.put(logicalDecodingMessageContentSchema, valueSchema);
        }

        return valueSchema;
    }

    private SchemaBuilder getSchemaBuilder(Schema logicalDecodingMessageContentSchema, Schema debeziumEventSchema) {
        // a schema name ending with such a suffix makes the record processable by Outbox SMT
        String schemaName = debeziumEventSchema.name() + Envelope.SCHEMA_NAME_SUFFIX;

        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(schemaName);

        for (Field originalSchemaField : debeziumEventSchema.fields()) {
            schemaBuilder.field(originalSchemaField.name(), originalSchemaField.schema());
        }

        schemaBuilder.field(Envelope.FieldName.AFTER, logicalDecodingMessageContentSchema);

        return schemaBuilder;
    }

    private Struct getUpdatedValue(Schema updatedValueSchema, Struct originalValue, Struct logicalDecodingMessageContent) {
        final Struct updatedValue = new Struct(updatedValueSchema);

        for (Field field : updatedValueSchema.fields()) {
            Object fieldValue;
            switch (field.name()) {
                case Envelope.FieldName.AFTER:
                    fieldValue = logicalDecodingMessageContent;
                    break;
                case Envelope.FieldName.OPERATION:
                    // replace the original operation so that a record will look as INSERT event
                    fieldValue = Envelope.Operation.CREATE.code();
                    break;
                default:
                    fieldValue = originalValue.get(field);
                    break;
            }
            updatedValue.put(field, fieldValue);
        }
        return updatedValue;
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }
}
