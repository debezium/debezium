/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteHandling;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.Strings;

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
 * <p>
 * The SMT also has the option to insert fields from the original record (e.g. 'op' or 'source.ts_ms' into the
 * unwrapped record or ad them as header attributes.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Jiri Pechanec
 */
public class ExtractNewRecordState<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractNewRecordState.class);

    private static final String PURPOSE = "source field insertion";
    private static final int SCHEMA_CACHE_SIZE = 64;
    private static final Pattern FIELD_SEPARATOR = Pattern.compile("\\.");

    private boolean dropTombstones;
    private DeleteHandling handleDeletes;
    private boolean addOperationHeader;
    private List<String> addSourceFields;
    private List<FieldReference> additionalHeaders;
    private List<FieldReference> additionalFields;
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

        addSourceFields = determineAdditionalSourceField(config.getString(ExtractNewRecordStateConfigDefinition.ADD_SOURCE_FIELDS));

        additionalFields = FieldReference.fromConfiguration(config.getString(ExtractNewRecordStateConfigDefinition.ADD_FIELDS));
        additionalHeaders = FieldReference.fromConfiguration(config.getString(ExtractNewRecordStateConfigDefinition.ADD_HEADERS));

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

    private static List<String> determineAdditionalSourceField(String addSourceFieldsConfig) {
        if (Strings.isNullOrEmpty(addSourceFieldsConfig)) {
            return Collections.emptyList();
        }
        else {
            return Arrays.stream(addSourceFieldsConfig.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public R apply(final R record) {
        Envelope.Operation operation;
        if (record.value() == null) {
            if (dropTombstones) {
                LOGGER.trace("Tombstone {} arrived and requested to be dropped", record.key());
                return null;
            }
            if (!additionalHeaders.isEmpty()) {
                Headers headersToAdd = makeHeaders(additionalHeaders, (Struct) record.value());
                headersToAdd.forEach(h -> record.headers().add(h));
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

        if (!additionalHeaders.isEmpty()) {
            Headers headersToAdd = makeHeaders(additionalHeaders, (Struct) record.value());
            headersToAdd.forEach(h -> record.headers().add(h));
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
                    oldRecord = !additionalFields.isEmpty() ? addFields(additionalFields, record, oldRecord) : addSourceFields(addSourceFields, record, oldRecord);

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

            newRecord = !additionalFields.isEmpty() ? addFields(additionalFields, record, newRecord) : addSourceFields(addSourceFields, record, newRecord);

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

    /**
     * Create an Headers object which contains the headers to be added.
     */
    private Headers makeHeaders(List<FieldReference> additionalHeaders, Struct originalRecordValue) {
        Headers headers = new ConnectHeaders();

        for (FieldReference fieldReference : additionalHeaders) {
            headers.add(fieldReference.getNewFieldName(), fieldReference.getValue(originalRecordValue),
                    fieldReference.getSchema(originalRecordValue.schema()));
        }

        return headers;
    }

    private R addFields(List<FieldReference> additionalFields, R originalRecord, R unwrappedRecord) {
        final Struct value = requireStruct(unwrappedRecord.value(), PURPOSE);
        Struct originalRecordValue = (Struct) originalRecord.value();

        Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(),
                s -> makeUpdatedSchema(additionalFields, value.schema(), originalRecordValue));

        // Update the value with the new fields
        Struct updatedValue = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));

        }

        for (FieldReference fieldReference : additionalFields) {
            updatedValue = updateValue(fieldReference, updatedValue, originalRecordValue);
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

    private Schema makeUpdatedSchema(List<FieldReference> additionalFields, Schema schema, Struct originalRecordValue) {
        // Get fields from original schema
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        // Update the schema with the new fields
        for (FieldReference fieldReference : additionalFields) {
            builder = updateSchema(fieldReference, builder, originalRecordValue.schema());
        }

        return builder.build();
    }

    private SchemaBuilder updateSchema(FieldReference fieldReference, SchemaBuilder builder, Schema originalRecordSchema) {
        return builder.field(fieldReference.getNewFieldName(), fieldReference.getSchema(originalRecordSchema));
    }

    private Struct updateValue(FieldReference fieldReference, Struct updatedValue, Struct struct) {
        return updatedValue.put(fieldReference.getNewFieldName(), fieldReference.getValue(struct));
    }

    private R addSourceFields(List<String> addSourceFields, R originalRecord, R unwrappedRecord) {
        // Return if no source fields to add
        if (addSourceFields.isEmpty()) {
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

    private Schema makeUpdatedSchema(Schema schema, Schema sourceSchema, List<String> addSourceFields) {
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

    /**
     * Represents a field that should be added to the outgoing record as a header
     * attribute or struct field.
     */
    private static class FieldReference {

        /**
         * The struct ("source", "transaction") hosting the given field, or {@code null} for "op" and "ts_ms".
         */
        private final String struct;

        /**
         * The simple field name.
         */
        private final String field;

        /**
         * The name for the outgoing attribute/field, e.g. "__op" or "__source_ts_ms".
         */
        private final String newFieldName;

        private FieldReference(String field) {
            String[] parts = FIELD_SEPARATOR.split(field);

            if (parts.length == 1) {
                this.struct = determineStruct(parts[0]);
                this.field = parts[0];
                this.newFieldName = ExtractNewRecordStateConfigDefinition.METADATA_FIELD_PREFIX + field;
            }
            else if (parts.length == 2) {
                this.struct = parts[0];

                if (!(this.struct.equals(Envelope.FieldName.SOURCE) || this.struct.equals(Envelope.FieldName.TRANSACTION))) {
                    throw new IllegalArgumentException("Unexpected field name: " + field);
                }

                this.field = parts[1];
                this.newFieldName = ExtractNewRecordStateConfigDefinition.METADATA_FIELD_PREFIX + this.struct + "_" + this.field;
            }
            else {
                throw new IllegalArgumentException("Unexpected field name: " + field);
            }
        }

        /**
         * Determines the struct hosting the given unqualified field.
         */
        private static String determineStruct(String simpleFieldName) {
            if (simpleFieldName.equals(Envelope.FieldName.OPERATION) || simpleFieldName.equals(Envelope.FieldName.TIMESTAMP)) {
                return null;
            }
            else if (simpleFieldName.equals(TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY) ||
                    simpleFieldName.equals(TransactionMonitor.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY) ||
                    simpleFieldName.equals(TransactionMonitor.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY)) {
                return Envelope.FieldName.TRANSACTION;
            }
            else {
                return Envelope.FieldName.SOURCE;
            }
        }

        static List<FieldReference> fromConfiguration(String addHeadersConfig) {
            if (Strings.isNullOrEmpty(addHeadersConfig)) {
                return Collections.emptyList();
            }
            else {
                return Arrays.stream(addHeadersConfig.split(","))
                        .map(String::trim)
                        .map(FieldReference::new)
                        .collect(Collectors.toList());
            }
        }

        String getNewFieldName() {
            return newFieldName;
        }

        Object getValue(Struct originalRecordValue) {
            Struct parentStruct = struct != null ? (Struct) originalRecordValue.get(struct) : originalRecordValue;

            // transaction is optional; e.g. not present during snapshotting atm.
            return parentStruct != null ? parentStruct.get(field) : null;
        }

        Schema getSchema(Schema originalRecordSchema) {
            Schema parentSchema = struct != null ? originalRecordSchema.field(struct).schema() : originalRecordSchema;

            org.apache.kafka.connect.data.Field schemaField = parentSchema.field(field);

            if (schemaField == null) {
                throw new IllegalArgumentException("Unexpected field name: " + field);
            }

            return SchemaUtil.copySchemaBasics(schemaField.schema()).optional().build();
        }
    }
}
