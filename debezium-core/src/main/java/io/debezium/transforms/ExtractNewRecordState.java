/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteHandling;
import io.debezium.util.BoundedConcurrentHashMap;

/**
 * Debezium generates CDC (<code>Envelope</code>) records that are struct of values containing values
 * <code>before</code> and <code>after change</code>. Sink connectors usually are not able to work
 * with a complex structure so a user use this SMT to extract <code>after</code> value and send it down
 * unwrapped in <code>Envelope</code>.
 * <p>
 * The functionality is similar to <code>ExtractField</code> SMT but has a special semantics for handling
 * delete events; when delete event is emitted by database then Debezium emits two messages: a delete
 * message and a tombstone message that serves as a signal to Kafka compaction process.
 * <p>
 * The SMT by default drops the tombstone message created by Debezium and converts the delete message into
 * a tombstone message that can be dropped, too, if required.
 * * <p>
 * The SMT also has the option to insert fields from the original record's 'source' struct into the new
 * unwrapped record prefixed with "__" (for example __lsn in Postgres, or __file in MySQL)
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Jiri Pechanec
 */
public class ExtractNewRecordState<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String PURPOSE = "source field insertion";
    private static final int SCHEMA_CACHE_SIZE = 64;
    private static final String OPERATION_FIELD_NAME = "op";
    private static final String TIMESTAMP_FIELD_NAME = "ts_ms";
    private static final String SOURCE_STRUCT_NAME = "source";

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractNewRecordState.class);

    private boolean dropTombstones;
    private DeleteHandling handleDeletes;
    private boolean addOperationHeader;
    private String[] addSourceFields;
    private String[] addFields;
    private String[] addHeaders;
    private Map<String, String[]> fieldStructMap;
    private String routeByField;
    private final ExtractField<R> afterDelegate = new ExtractField.Value<R>();
    private final ExtractField<R> beforeDelegate = new ExtractField.Value<R>();
    private final InsertField<R> removedDelegate = new InsertField.Value<R>();
    private final InsertField<R> updatedDelegate = new InsertField.Value<R>();
    private BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache;
    private SmtManager<R> smtManager;

    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);

        final Field.Set configFields = Field.setOf(ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES, ExtractNewRecordStateConfigDefinition.HANDLE_DELETES);
        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        dropTombstones = config.getBoolean(ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES);
        handleDeletes = DeleteHandling.parse(config.getString(ExtractNewRecordStateConfigDefinition.HANDLE_DELETES));

        addOperationHeader = config.getBoolean(ExtractNewRecordStateConfigDefinition.OPERATION_HEADER);

        addSourceFields = config.getString(ExtractNewRecordStateConfigDefinition.ADD_SOURCE_FIELDS).isEmpty() ? null
                : config.getString(ExtractNewRecordStateConfigDefinition.ADD_SOURCE_FIELDS).split(",");

        addFields = config.getString(ExtractNewRecordStateConfigDefinition.ADD_FIELDS).isEmpty() ? null
                : config.getString(ExtractNewRecordStateConfigDefinition.ADD_FIELDS).split(",");

        addHeaders = config.getString(ExtractNewRecordStateConfigDefinition.ADD_HEADERS).isEmpty() ? null
                : config.getString(ExtractNewRecordStateConfigDefinition.ADD_HEADERS).split(",");

        fieldStructMap = new HashMap<>();

        String routeFieldConfig = config.getString(ExtractNewRecordStateConfigDefinition.ROUTE_BY_FIELD);
        routeByField = routeFieldConfig.isEmpty() ? null : routeFieldConfig;

        Map<String, String> delegateConfig = new LinkedHashMap<>();
        delegateConfig.put("field", "before");
        beforeDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("field", "after");
        afterDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", ExtractNewRecordStateConfigDefinition.DELETED_FIELD);
        delegateConfig.put("static.value", "true");
        removedDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", ExtractNewRecordStateConfigDefinition.DELETED_FIELD);
        delegateConfig.put("static.value", "false");
        updatedDelegate.configure(delegateConfig);

        schemaUpdateCache = new BoundedConcurrentHashMap<>(SCHEMA_CACHE_SIZE);
    }

    @Override
    public R apply(final R record) {
        Envelope.Operation operation;
        if (record.value() == null) {
            if (dropTombstones) {
                LOGGER.trace("Tombstone {} arrived and requested to be dropped", record.key());
                return null;
            }
            if (addHeaders != null) {
                fieldStructMap = makeFieldMap(addHeaders);
                Headers headersToAdd = makeHeaders(fieldStructMap, (Struct) record.value());

                for (String header : addHeaders) {
                    record.headers().add(headersToAdd.lastWithName(makeFieldName(fieldStructMap.get(header))));
                }
            }
            else if (addOperationHeader) {
                operation = Envelope.Operation.DELETE;
                LOGGER.warn("operation.header has been deprecated and is scheduled for removal. Use add.headers instead.");
                record.headers().addString(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY, operation.toString());
            }
            return record;
        }

        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        if (addHeaders != null) {
            fieldStructMap = makeFieldMap(addHeaders);
            Headers headersToAdd = makeHeaders(fieldStructMap, (Struct) record.value());

            for (String header : addHeaders) {
                record.headers().add(headersToAdd.lastWithName(makeFieldName(fieldStructMap.get(header))));
            }
        }
        else if (addOperationHeader) {
            LOGGER.warn("operation.header has been deprecated and is scheduled for removal. Use add.headers instead.");
            String operationString = ((Struct) record.value()).getString("op");
            operation = Envelope.Operation.forCode(operationString);

            if (operationString.isEmpty() || operation == null) {
                LOGGER.warn("Unknown operation thus unable to add the operation header into the message");
            }
            else {
                record.headers().addString(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY, operation.code());
            }
        }

        R newRecord = afterDelegate.apply(record);
        if (newRecord.value() == null) {
            if (routeByField != null) {
                Struct recordValue = requireStruct(record.value(), "Read record to set topic routing for DELETE");
                String newTopicName = recordValue.getStruct("before").getString(routeByField);
                newRecord = setTopic(newTopicName, newRecord);
            }

            // Handling delete records
            switch (handleDeletes) {
                case DROP:
                    LOGGER.trace("Delete message {} requested to be dropped", record.key());
                    return null;
                case REWRITE:
                    LOGGER.trace("Delete message {} requested to be rewritten", record.key());
                    R oldRecord = beforeDelegate.apply(record);
                    oldRecord = addFields != null ? addFields(addFields, record, oldRecord) : addSourceFields(addSourceFields, record, oldRecord);

                    return removedDelegate.apply(oldRecord);
                default:
                    return newRecord;
            }
        }
        else {
            // Add on any requested source fields from the original record to the new unwrapped record
            if (routeByField != null) {
                Struct recordValue = requireStruct(newRecord.value(), "Read record to set topic routing for CREATE / UPDATE");
                String newTopicName = recordValue.getString(routeByField);
                newRecord = setTopic(newTopicName, newRecord);
            }

            newRecord = addFields != null ? addFields(addFields, record, newRecord) : addSourceFields(addSourceFields, record, newRecord);

            // Handling insert and update records
            switch (handleDeletes) {
                case REWRITE:
                    LOGGER.trace("Insert/update message {} requested to be rewritten", record.key());
                    return updatedDelegate.apply(newRecord);
                default:
                    return newRecord;
            }
        }
    }

    private R setTopic(String updatedTopicValue, R record) {
        String topicName = updatedTopicValue == null ? record.topic() : updatedTopicValue;

        return record.newRecord(
                topicName,
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp());
    }

    private Headers makeHeaders(Map<String, String[]> fieldStructMap, Struct originalRecordValue) {
        // Create an Headers object which contains the headers to be added
        Headers headers = new ConnectHeaders();
        String[] fieldNameParts;
        String fieldName;

        for (String field : fieldStructMap.keySet()) {
            fieldNameParts = fieldStructMap.get(field);
            fieldName = getFieldName(fieldNameParts);

            if (field.equals(OPERATION_FIELD_NAME) || field.equals(TIMESTAMP_FIELD_NAME)) {
                headers.add(makeFieldName(fieldNameParts), originalRecordValue.get(fieldName), originalRecordValue.schema().field(fieldName).schema());
            }
            else {
                Struct struct = originalRecordValue.getStruct(SOURCE_STRUCT_NAME);
                if (struct.schema().field(fieldName) == null) {
                    throw new ConfigException("Field specified in 'add.headers' does not exist: " + field);
                }
                headers.add(makeFieldName(fieldNameParts), struct.get(fieldName), struct.schema().field(fieldName).schema());
            }
        }

        return headers;
    }

    private R addFields(String[] addFields, R originalRecord, R unwrappedRecord) {
        // Return if no fields to add
        if (addFields == null) {
            return unwrappedRecord;
        }

        Map<String, String[]> fieldStructMap = makeFieldMap(addFields);

        final Struct value = requireStruct(unwrappedRecord.value(), PURPOSE);
        Struct originalRecordValue = (Struct) originalRecord.value();

        Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(),
                s -> makeUpdatedSchema(fieldStructMap, value.schema(), originalRecordValue));

        // Update the value with the new fields
        Struct updatedValue = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));

        }

        for (String field : addFields) {

            if (field.equals(OPERATION_FIELD_NAME) || field.equals(TIMESTAMP_FIELD_NAME)) {
                updatedValue = updateValue(fieldStructMap.get(field), updatedValue, originalRecordValue);
            }
            else {
                updatedValue = updateValue(fieldStructMap.get(field), updatedValue, originalRecordValue.getStruct(SOURCE_STRUCT_NAME));
            }
        }

        return unwrappedRecord.newRecord(
                unwrappedRecord.topic(),
                unwrappedRecord.kafkaPartition(),
                unwrappedRecord.keySchema(),
                unwrappedRecord.key(),
                updatedSchema,
                updatedValue,
                unwrappedRecord.timestamp());
    }

    private Schema makeUpdatedSchema(Map<String, String[]> fieldStructMap, Schema schema, Struct originalRecordValue) {
        // Get fields from original schema
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        // Update the schema with the new fields
        for (String field : fieldStructMap.keySet()) {

            if (field.equals(OPERATION_FIELD_NAME) || field.equals(TIMESTAMP_FIELD_NAME)) {
                builder = updateSchema(fieldStructMap.get(field), field, builder, originalRecordValue.schema());
            }
            else {
                builder = updateSchema(fieldStructMap.get(field), field, builder, originalRecordValue.getStruct(SOURCE_STRUCT_NAME).schema());
            }
        }

        return builder.build();
    }

    private SchemaBuilder updateSchema(String[] fieldNameParts, String field, SchemaBuilder builder, Schema structSchema) {
        String fieldName = getFieldName(fieldNameParts);

        if (structSchema.field(fieldName) == null) {
            throw new ConfigException("Field specified in 'add.fields' does not exist: " + field);
        }
        builder.field(
                makeFieldName(fieldNameParts), structSchema.field(fieldName).schema());

        return builder;
    }

    private Struct updateValue(String[] fieldNameParts, Struct updatedValue, Struct struct) {
        String fieldName = getFieldName(fieldNameParts);

        return updatedValue.put(makeFieldName(fieldNameParts), struct.get(fieldName));
    }

    private Map<String, String[]> makeFieldMap(String[] fields) {
        // Convert the provided fields into a Map with the provided value as key and an the struct and field as value (in an array)
        Map<String, String[]> fieldMap = new LinkedHashMap<>();

        for (String field : fields) {
            fieldMap.computeIfAbsent(field, s -> field.split("\\."));
        }
        return fieldMap;
    }

    private String getFieldName(String[] fieldNameParts) {
        // Returns the field name without an (optional) struct specification
        return fieldNameParts[fieldNameParts.length - 1];
    }

    private String makeFieldName(String[] fieldNameParts) {
        // Makes a name for the field in the unwrapped record
        if (fieldNameParts.length > 1) {
            return ExtractNewRecordStateConfigDefinition.METADATA_FIELD_PREFIX + fieldNameParts[0] +
                    ExtractNewRecordStateConfigDefinition.METADATA_FIELD_PREFIX + fieldNameParts[1];
        }
        else {
            return ExtractNewRecordStateConfigDefinition.METADATA_FIELD_PREFIX + fieldNameParts[0];
        }
    }

    private R addSourceFields(String[] addSourceFields, R originalRecord, R unwrappedRecord) {
        // Return if no source fields to add
        if (addSourceFields == null) {
            return unwrappedRecord;
        }

        LOGGER.warn("add.source.fields has been deprecated and is scheduled for removal. Use add.fields instead.");

        final Struct value = requireStruct(unwrappedRecord.value(), PURPOSE);
        Struct source = ((Struct) originalRecord.value()).getStruct("source");

        // Get (or compute) the updated schema from the cache
        Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(), s -> makeUpdatedSchema(s, source.schema(), addSourceFields));

        // Create the updated struct
        final Struct updatedValue = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        for (String sourceField : addSourceFields) {
            updatedValue.put(ExtractNewRecordStateConfigDefinition.METADATA_FIELD_PREFIX + sourceField, source.get(sourceField));
        }

        return unwrappedRecord.newRecord(
                unwrappedRecord.topic(),
                unwrappedRecord.kafkaPartition(),
                unwrappedRecord.keySchema(),
                unwrappedRecord.key(),
                updatedSchema,
                updatedValue,
                unwrappedRecord.timestamp());
    }

    private Schema makeUpdatedSchema(Schema schema, Schema sourceSchema, String[] addSourceFields) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        // Get fields from original schema
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        // Add the requested source fields, throw exception if a specified source field is not part of the source schema
        for (String sourceField : addSourceFields) {
            if (sourceSchema.field(sourceField) == null) {
                throw new ConfigException("Source field specified in 'add.source.fields' does not exist: " + sourceField);
            }
            builder.field(
                    ExtractNewRecordStateConfigDefinition.METADATA_FIELD_PREFIX + sourceField,
                    sourceSchema.field(sourceField).schema());
        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES, ExtractNewRecordStateConfigDefinition.HANDLE_DELETES,
                ExtractNewRecordStateConfigDefinition.OPERATION_HEADER);
        return config;
    }

    @Override
    public void close() {
        beforeDelegate.close();
        afterDelegate.close();
        removedDelegate.close();
        updatedDelegate.close();
    }

}
