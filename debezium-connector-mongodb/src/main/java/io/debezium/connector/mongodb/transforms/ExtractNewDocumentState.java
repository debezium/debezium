/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Flatten;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.FieldNameAdjustmentMode;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.MongoDbFieldName;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteHandling;
import io.debezium.transforms.SmtManager;
import io.debezium.util.Strings;

/**
 * Debezium Mongo Connector generates the CDC records in String format. Sink connectors usually are not able to parse
 * the string and insert the document as it is represented in the Source. so a user use this SMT to parse the String
 * and insert the MongoDB document in the JSON format.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Sairam Polavarapu
 * @author Renato mefi
 */
public class ExtractNewDocumentState<R extends ConnectRecord<R>> implements Transformation<R> {

    public enum ArrayEncoding implements EnumeratedValue {
        ARRAY("array"),
        DOCUMENT("document");

        private final String value;

        ArrayEncoding(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static ArrayEncoding parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (ArrayEncoding option : ArrayEncoding.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static ArrayEncoding parse(String value, String defaultValue) {
            ArrayEncoding mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractNewDocumentState.class);
    private static final Pattern FIELD_SEPARATOR = Pattern.compile("\\.");
    private static final Pattern NEW_FIELD_SEPARATOR = Pattern.compile(":");

    private static final Field ARRAY_ENCODING = Field.create("array.encoding")
            .withDisplayName("Array encoding")
            .withEnum(ArrayEncoding.class, ArrayEncoding.ARRAY)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The arrays can be encoded using 'array' schema type (the default) or as a 'document' (similar to how BSON encodes arrays). "
                    + "'array' is easier to consume but requires all elements in the array to be of the same type. "
                    + "Use 'document' if the arrays in data source mix different types together.");

    private static final Field FLATTEN_STRUCT = Field.create("flatten.struct")
            .withDisplayName("Flatten struct")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Flattening structs by concatenating the fields into plain properties, using a "
                    + "(configurable) delimiter.");

    private static final Field DELIMITER = Field.create("flatten.struct.delimiter")
            .withDisplayName("Delimiter for flattened struct")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("_")
            .withDescription("Delimiter to concat between field names from the input record when generating field names for the"
                    + "output record.");

    private final ExtractField<R> beforeExtractor = new ExtractField.Value<>();
    private final ExtractField<R> afterExtractor = new ExtractField.Value<>();
    private final ExtractField<R> updateDescriptionExtractor = new ExtractField.Value<>();
    private final ExtractField<R> keyExtractor = new ExtractField.Key<>();

    private MongoDataConverter converter;
    private final Flatten<R> recordFlattener = new Flatten.Value<>();

    private List<FieldReference> additionalHeaders;
    private List<FieldReference> additionalFields;
    private boolean flattenStruct;
    private String delimiter;

    private boolean dropTombstones;
    private DeleteHandling handleDeletes;

    private SmtManager<R> smtManager;

    private final Field.Set configFields = Field.setOf(ARRAY_ENCODING, FLATTEN_STRUCT, DELIMITER,
            ExtractNewRecordStateConfigDefinition.HANDLE_DELETES,
            ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES,
            ExtractNewRecordStateConfigDefinition.ADD_HEADERS,
            ExtractNewRecordStateConfigDefinition.ADD_HEADERS_PREFIX,
            ExtractNewRecordStateConfigDefinition.ADD_FIELDS,
            ExtractNewRecordStateConfigDefinition.ADD_FIELDS_PREFIX);

    @Override
    public R apply(R record) {
        if (!smtManager.isValidKey(record)) {
            return record;
        }

        final R keyRecord = keyExtractor.apply(record);

        BsonDocument keyDocument = BsonDocument.parse("{ \"id\" : " + keyRecord.key().toString() + "}");
        BsonDocument valueDocument = new BsonDocument();

        // Tombstone message
        if (record.value() == null) {
            if (dropTombstones) {
                LOGGER.trace("Tombstone {} arrived and requested to be dropped", record.key());
                return null;
            }
            if (!additionalHeaders.isEmpty()) {
                Headers headersToAdd = makeHeaders(additionalHeaders, (Struct) record.value());
                headersToAdd.forEach(h -> record.headers().add(h));
            }
            return newRecord(record, keyDocument, valueDocument);
        }

        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        final R beforeRecord = beforeExtractor.apply(record);
        final R afterRecord = afterExtractor.apply(record);
        final R updateDescriptionRecord = updateDescriptionExtractor.apply(record);

        if (!additionalHeaders.isEmpty()) {
            Headers headersToAdd = makeHeaders(additionalHeaders, (Struct) record.value());
            headersToAdd.forEach(h -> record.headers().add(h));
        }

        // insert || replace || update with capture.mode="change_streams_update_full" or "change_streams_update_full_with_pre_image"
        if (afterRecord.value() != null) {
            valueDocument = getFullDocument(afterRecord, keyDocument);
        }

        // update
        if (afterRecord.value() == null && updateDescriptionRecord.value() != null) {
            valueDocument = getPartialUpdateDocument(beforeRecord, updateDescriptionRecord, keyDocument);
        }

        boolean isDeletion = false;
        // delete
        if (afterRecord.value() == null && updateDescriptionRecord.value() == null) {
            if (handleDeletes.equals(DeleteHandling.DROP)) {
                LOGGER.trace("Delete {} arrived and requested to be dropped", record.key());
                return null;
            }

            if (beforeRecord.value() != null && handleDeletes.equals(DeleteHandling.REWRITE)) {
                valueDocument = getFullDocument(beforeRecord, keyDocument);
            }

            isDeletion = true;
        }

        if (handleDeletes.equals(DeleteHandling.REWRITE)) {
            valueDocument.append(ExtractNewRecordStateConfigDefinition.DELETED_FIELD, new BsonBoolean(isDeletion));
        }

        return newRecord(record, keyDocument, valueDocument);
    }

    private R newRecord(R record, BsonDocument keyDocument, BsonDocument valueDocument) {
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
        Set<Entry<String, BsonValue>> keyPairs = keyDocument.entrySet();
        for (Entry<String, BsonValue> keyPairsForSchema : keyPairs) {
            converter.addFieldSchema(keyPairsForSchema, keySchemaBuilder);
        }

        Schema finalKeySchema = keySchemaBuilder.build();
        Struct finalKeyStruct = new Struct(finalKeySchema);

        for (Entry<String, BsonValue> keyPairsForStruct : keyPairs) {
            converter.convertRecord(keyPairsForStruct, finalKeySchema, finalKeyStruct);
        }

        Schema finalValueSchema = null;
        Struct finalValueStruct = null;

        if (valueDocument.size() > 0) {
            String newValueSchemaName = record.valueSchema().name();
            if (Envelope.isEnvelopeSchema(newValueSchemaName)) {
                newValueSchemaName = newValueSchemaName.substring(0, newValueSchemaName.length() - 9);
            }
            SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().name(newValueSchemaName);

            Set<Entry<String, BsonValue>> valuePairs = valueDocument.entrySet();
            for (Entry<String, BsonValue> valuePairsForSchema : valuePairs) {
                converter.addFieldSchema(valuePairsForSchema, valueSchemaBuilder);
            }

            if (!additionalFields.isEmpty()) {
                addAdditionalFieldsSchema(additionalFields, record, valueSchemaBuilder);
            }

            finalValueSchema = valueSchemaBuilder.build();
            finalValueStruct = new Struct(finalValueSchema);
            for (Entry<String, BsonValue> valuePairsForStruct : valuePairs) {
                converter.convertRecord(valuePairsForStruct, finalValueSchema, finalValueStruct);

            }

            if (!additionalFields.isEmpty()) {
                addFields(additionalFields, record, finalValueStruct);
            }
        }

        R newRecord = record.newRecord(record.topic(), record.kafkaPartition(), finalKeySchema,
                finalKeyStruct, finalValueSchema, finalValueStruct, record.timestamp());

        if (flattenStruct) {
            return recordFlattener.apply(newRecord);
        }

        return newRecord;
    }

    private void addAdditionalFieldsSchema(List<FieldReference> additionalFields, R originalRecord, SchemaBuilder valueSchemaBuilder) {
        Schema sourceSchema = originalRecord.valueSchema();
        for (FieldReference fieldReference : additionalFields) {
            valueSchemaBuilder.field(fieldReference.newFieldName, fieldReference.getSchema(sourceSchema));
        }
    }

    private void addFields(List<FieldReference> additionalFields, R originalRecord, Struct value) {
        Struct originalRecordValue = (Struct) originalRecord.value();

        // Update the value with the new fields
        for (FieldReference fieldReference : additionalFields) {
            value.put(fieldReference.newFieldName, fieldReference.getValue(originalRecordValue));
        }
    }

    private BsonDocument getPartialUpdateDocument(R beforeRecord, R updateDescriptionRecord, BsonDocument keyDocument) {
        BsonDocument valueDocument = new BsonDocument();

        Struct updateDescription = requireStruct(updateDescriptionRecord.value(), MongoDbFieldName.UPDATE_DESCRIPTION);

        String updated = updateDescription.getString(MongoDbFieldName.UPDATED_FIELDS);
        List<String> removed = updateDescription.getArray(MongoDbFieldName.REMOVED_FIELDS);

        if (beforeRecord.value() != null) {
            valueDocument = BsonDocument.parse(beforeRecord.value().toString());
        }

        if (updated != null) {
            BsonDocument updatedBson = BsonDocument.parse(updated);
            for (Entry<String, BsonValue> valueEntry : updatedBson.entrySet()) {
                valueDocument.append(valueEntry.getKey(), valueEntry.getValue());
            }
        }

        if (removed != null) {
            for (String field : removed) {
                valueDocument.keySet().remove(field);
            }
        }

        if (!valueDocument.containsKey("_id")) {
            valueDocument.append("_id", keyDocument.get("id"));
        }

        if (flattenStruct) {
            final BsonDocument newDocument = new BsonDocument();
            valueDocument.forEach((fKey, fValue) -> newDocument.put(fKey.replace(".", delimiter), fValue));
            valueDocument = newDocument;
        }

        return valueDocument;
    }

    private BsonDocument getFullDocument(R record, BsonDocument key) {
        return BsonDocument.parse(record.value().toString());
    }

    private Headers makeHeaders(List<FieldReference> additionalHeaders, Struct originalRecordValue) {
        Headers headers = new ConnectHeaders();

        for (FieldReference fieldReference : additionalHeaders) {
            // add "d" operation header to tombstone events
            if (originalRecordValue == null) {
                if (Envelope.FieldName.OPERATION.equals(fieldReference.field)) {
                    headers.addString(fieldReference.newFieldName, Operation.DELETE.code());
                }
                continue;
            }
            if (fieldReference.exists(originalRecordValue.schema())) {
                headers.add(fieldReference.getNewFieldName(), fieldReference.getValue(originalRecordValue),
                        fieldReference.getSchema(originalRecordValue.schema()));
            }
        }

        return headers;
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
    public void configure(final Map<String, ?> map) {
        final Configuration config = Configuration.from(map);
        smtManager = new SmtManager<>(config);

        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new DebeziumException("Unable to validate config.");
        }

        FieldNameAdjustmentMode fieldNameAdjustmentMode = FieldNameAdjustmentMode.parse(
                config.getString(CommonConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE));
        SchemaNameAdjuster fieldNameAdjuster = fieldNameAdjustmentMode.createAdjuster();
        converter = new MongoDataConverter(
                ArrayEncoding.parse(config.getString(ARRAY_ENCODING)),
                FieldNameSelector.defaultNonRelationalSelector(fieldNameAdjuster),
                fieldNameAdjustmentMode != FieldNameAdjustmentMode.NONE);

        String addFieldsPrefix = config.getString(ExtractNewRecordStateConfigDefinition.ADD_FIELDS_PREFIX);
        String addHeadersPrefix = config.getString(ExtractNewRecordStateConfigDefinition.ADD_HEADERS_PREFIX);
        additionalHeaders = FieldReference.fromConfiguration(addHeadersPrefix, config.getString(ExtractNewRecordStateConfigDefinition.ADD_HEADERS));
        additionalFields = FieldReference.fromConfiguration(addFieldsPrefix, config.getString(ExtractNewRecordStateConfigDefinition.ADD_FIELDS));

        flattenStruct = config.getBoolean(FLATTEN_STRUCT);
        delimiter = config.getString(DELIMITER);

        dropTombstones = config.getBoolean(ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES);
        handleDeletes = DeleteHandling.parse(config.getString(ExtractNewRecordStateConfigDefinition.HANDLE_DELETES));

        final Map<String, String> beforeExtractorConfig = new HashMap<>();
        beforeExtractorConfig.put("field", FieldName.BEFORE);
        final Map<String, String> afterExtractorConfig = new HashMap<>();
        afterExtractorConfig.put("field", FieldName.AFTER);
        final Map<String, String> updateDescriptionExtractorConfig = new HashMap<>();
        updateDescriptionExtractorConfig.put("field", MongoDbFieldName.UPDATE_DESCRIPTION);
        final Map<String, String> keyExtractorConfig = new HashMap<>();
        keyExtractorConfig.put("field", "id");

        beforeExtractor.configure(beforeExtractorConfig);
        afterExtractor.configure(afterExtractorConfig);
        updateDescriptionExtractor.configure(updateDescriptionExtractorConfig);
        keyExtractor.configure(keyExtractorConfig);

        final Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("delimiter", delimiter);
        recordFlattener.configure(delegateConfig);
    }

    private static List<String> determineAdditionalSourceField(String addSourceFieldsConfig) {
        if (Strings.isNullOrEmpty(addSourceFieldsConfig)) {
            return Collections.emptyList();
        }
        return Arrays.stream(addSourceFieldsConfig.split(",")).map(String::trim).collect(Collectors.toList());
    }

    /**
     * Represents a field that should be added to the outgoing record as a header attribute or struct field.
     */
    // todo: refactor with ExtractNewRecordState
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

        private FieldReference(String prefix, String field) {
            String[] parts = NEW_FIELD_SEPARATOR.split(field);
            String[] splits = FIELD_SEPARATOR.split(parts[0]);
            this.field = splits.length == 1 ? splits[0] : splits[1];
            this.struct = (splits.length == 1) ? determineStruct(this.field) : splits[0];

            if (parts.length == 1) {
                this.newFieldName = prefix + (splits.length == 1 ? this.field : this.struct + "_" + this.field);
            }
            else if (parts.length == 2) {
                this.newFieldName = prefix + parts[1];
            }
            else {
                throw new IllegalArgumentException("Unexpected field name: " + field);
            }
        }

        /**
         * Determine the struct hosting the given unqualified field.
         */
        private static String determineStruct(String simpleFieldName) {
            switch (simpleFieldName) {
                case FieldName.OPERATION:
                case FieldName.TIMESTAMP:
                    return null;
                case TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY:
                case TransactionMonitor.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY:
                case TransactionMonitor.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY:
                    return FieldName.TRANSACTION;
                case MongoDbFieldName.UPDATE_DESCRIPTION:
                    return MongoDbFieldName.UPDATE_DESCRIPTION;
                default:
                    return FieldName.SOURCE;
            }
        }

        static List<FieldReference> fromConfiguration(String fieldPrefix, String addHeadersConfig) {
            if (Strings.isNullOrEmpty(addHeadersConfig)) {
                return Collections.emptyList();
            }
            else {
                return Arrays.stream(addHeadersConfig.split(","))
                        .map(String::trim)
                        .map(field -> new FieldReference(fieldPrefix, field))
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

        private boolean exists(Schema originalRecordSchema) {
            Schema parentSchema = struct != null ? originalRecordSchema.field(struct).schema() : originalRecordSchema;

            org.apache.kafka.connect.data.Field schemaField = parentSchema.field(field);

            return schemaField != null;
        }
    }
}
