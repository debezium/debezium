/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

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
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Flatten;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.FieldNameSelector;
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

    public static final Field SANITIZE_FIELD_NAMES = Field.create("sanitize.field.names")
            .withDisplayName("Sanitize field names to adhere to Avro naming conventions")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Whether field names will be sanitized to Avro naming conventions")
            .withDefault(Boolean.FALSE);

    public static final Field ADD_SOURCE_FIELDS = Field.create("add.source.fields")
            .withDisplayName("Adds the specified fields from the 'source' field from the payload if they exist.")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("")
            .withDescription("DEPRECATED. Please use the 'add.fields' option instead. "
                    + "Adds each field listed from the 'source' element of the payload, prefixed with __ "
                    + "Example: 'version,connector' would add __version and __connector fields");

    public static final Field OPERATION_HEADER = Field.create("operation.header")
            .withDisplayName("Adds a message header representing the applied operation")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("DEPRECATED. Please use the 'add.fields' option instead. "
                    + "Adds the operation type of the change event as a header."
                    + "Its key is '" + ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY + "'");

    private final ExtractField<R> afterExtractor = new ExtractField.Value<>();
    private final ExtractField<R> patchExtractor = new ExtractField.Value<>();
    private final ExtractField<R> keyExtractor = new ExtractField.Key<>();

    private MongoDataConverter converter;
    private final Flatten<R> recordFlattener = new Flatten.Value<>();

    private boolean addOperationHeader;
    private List<String> addSourceFields;
    private List<FieldReference> additionalHeaders;
    private List<FieldReference> additionalFields;
    private boolean flattenStruct;
    private String delimiter;

    private boolean dropTombstones;
    private DeleteHandling handleDeletes;

    private SmtManager<R> smtManager;

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
            else if (addOperationHeader) {
                LOGGER.warn("operation.header has been deprecated and is scheduled for removal.  Use add.headers instead.");
                record.headers().addString(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY, Operation.DELETE.code());
            }
            return newRecord(record, keyDocument, valueDocument);
        }

        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        final R afterRecord = afterExtractor.apply(record);
        final R patchRecord = patchExtractor.apply(record);

        if (!additionalHeaders.isEmpty()) {
            Headers headersToAdd = makeHeaders(additionalHeaders, (Struct) record.value());
            headersToAdd.forEach(h -> record.headers().add(h));
        }
        else if (addOperationHeader) {
            LOGGER.warn("operation.header has been deprecated and is scheduled for removal.  Use add.headers instead.");
            record.headers().addString(ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY, ((Struct) record.value()).get("op").toString());
        }

        // insert
        if (afterRecord.value() != null) {
            valueDocument = getInsertDocument(afterRecord, keyDocument);
        }

        // update
        if (afterRecord.value() == null && patchRecord.value() != null) {
            valueDocument = getUpdateDocument(patchRecord, keyDocument);
        }

        boolean isDeletion = false;
        // delete
        if (afterRecord.value() == null && patchRecord.value() == null) {
            if (handleDeletes.equals(DeleteHandling.DROP)) {
                LOGGER.trace("Delete {} arrived and requested to be dropped", record.key());
                return null;
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
                if (valuePairsForSchema.getKey().equalsIgnoreCase("$set")) {
                    BsonDocument val1 = BsonDocument.parse(valuePairsForSchema.getValue().toString());
                    Set<Entry<String, BsonValue>> keyValuesForSetSchema = val1.entrySet();
                    for (Entry<String, BsonValue> keyValuesForSetSchemaEntry : keyValuesForSetSchema) {
                        converter.addFieldSchema(keyValuesForSetSchemaEntry, valueSchemaBuilder);
                    }
                }
                else {
                    converter.addFieldSchema(valuePairsForSchema, valueSchemaBuilder);
                }
            }

            if (addSourceFields != null) {
                addSourceFieldsSchema(addSourceFields, record, valueSchemaBuilder);
            }

            if (!additionalFields.isEmpty()) {
                addAdditionalFieldsSchema(additionalFields, record, valueSchemaBuilder);
            }

            finalValueSchema = valueSchemaBuilder.build();
            finalValueStruct = new Struct(finalValueSchema);
            for (Entry<String, BsonValue> valuePairsForStruct : valuePairs) {
                if (valuePairsForStruct.getKey().equalsIgnoreCase("$set")) {
                    BsonDocument val1 = BsonDocument.parse(valuePairsForStruct.getValue().toString());
                    Set<Entry<String, BsonValue>> keyValueForSetStruct = val1.entrySet();
                    for (Entry<String, BsonValue> keyValueForSetStructEntry : keyValueForSetStruct) {
                        converter.convertRecord(keyValueForSetStructEntry, finalValueSchema, finalValueStruct);
                    }
                }
                else {
                    converter.convertRecord(valuePairsForStruct, finalValueSchema, finalValueStruct);
                }
            }

            if (addSourceFields != null) {
                addSourceFieldsValue(addSourceFields, record, finalValueStruct);
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

    private void addSourceFieldsSchema(List<String> addSourceFields, R originalRecord, SchemaBuilder valueSchemaBuilder) {
        Schema sourceSchema = originalRecord.valueSchema().field("source").schema();
        for (String sourceField : addSourceFields) {
            if (sourceSchema.field(sourceField) == null) {
                throw new ConfigException("Source field specified in 'add.source.fields' does not exist: " + sourceField);
            }
            valueSchemaBuilder.field(ExtractNewRecordStateConfigDefinition.METADATA_FIELD_PREFIX + sourceField,
                    sourceSchema.field(sourceField).schema());
        }
    }

    private void addAdditionalFieldsSchema(List<FieldReference> additionalFields, R originalRecord, SchemaBuilder valueSchemaBuilder) {
        Schema sourceSchema = originalRecord.valueSchema();
        for (FieldReference fieldReference : additionalFields) {
            valueSchemaBuilder.field(fieldReference.newFieldName, fieldReference.getSchema(sourceSchema));
        }
    }

    private void addSourceFieldsValue(List<String> addSourceFields, R originalRecord, Struct valueStruct) {
        Struct sourceValue = ((Struct) originalRecord.value()).getStruct("source");
        for (String sourceField : addSourceFields) {
            valueStruct.put(ExtractNewRecordStateConfigDefinition.METADATA_FIELD_PREFIX + sourceField,
                    sourceValue.get(sourceField));
        }
    }

    private void addFields(List<FieldReference> additionalFields, R originalRecord, Struct value) {
        Struct originalRecordValue = (Struct) originalRecord.value();

        // Update the value with the new fields
        for (FieldReference fieldReference : additionalFields) {
            value.put(fieldReference.newFieldName, fieldReference.getValue(originalRecordValue));
        }
    }

    private BsonDocument getUpdateDocument(R patchRecord, BsonDocument keyDocument) {
        BsonDocument valueDocument = new BsonDocument();
        BsonDocument document = BsonDocument.parse(patchRecord.value().toString());

        if (document.containsKey("$set")) {
            valueDocument = document.getDocument("$set");
        }

        if (document.containsKey("$unset")) {
            Set<Entry<String, BsonValue>> unsetDocumentEntry = document.getDocument("$unset").entrySet();

            for (Entry<String, BsonValue> valueEntry : unsetDocumentEntry) {
                // In case unset of a key is false we don't have to do anything with it,
                // if it's true we want to set the value to null
                if (!valueEntry.getValue().asBoolean().getValue()) {
                    continue;
                }
                valueDocument.append(valueEntry.getKey(), new BsonNull());
            }
        }

        if (!document.containsKey("$set") && !document.containsKey("$unset")) {
            if (!document.containsKey("_id")) {
                throw new ConnectException("Unable to process Mongo Operation, a '$set' or '$unset' is necessary " +
                        "for partial updates or '_id' is expected for full Document replaces.");
            }
            // In case of a full update we can use the whole Document as it is
            // see https://docs.mongodb.com/manual/reference/method/db.collection.update/#replace-a-document-entirely
            valueDocument = document;
            valueDocument.remove("_id");
        }

        if (!valueDocument.containsKey("id")) {
            valueDocument.append("id", keyDocument.get("id"));
        }

        if (flattenStruct) {
            final BsonDocument newDocument = new BsonDocument();
            valueDocument.forEach((fKey, fValue) -> newDocument.put(fKey.replace(".", delimiter), fValue));
            valueDocument = newDocument;
        }

        return valueDocument;
    }

    private BsonDocument getInsertDocument(R record, BsonDocument key) {
        BsonDocument valueDocument = BsonDocument.parse(record.value().toString());
        valueDocument.remove("_id");
        valueDocument.append("id", key.get("id"));

        return valueDocument;
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
            headers.add(fieldReference.getNewFieldName(), fieldReference.getValue(originalRecordValue),
                    fieldReference.getSchema(originalRecordValue.schema()));
        }

        return headers;
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null,
                ARRAY_ENCODING,
                FLATTEN_STRUCT,
                DELIMITER,
                SANITIZE_FIELD_NAMES);
        return config;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> map) {
        final Configuration config = Configuration.from(map);
        smtManager = new SmtManager<>(config);

        final Field.Set configFields = Field.setOf(ARRAY_ENCODING, FLATTEN_STRUCT, DELIMITER,
                OPERATION_HEADER,
                ADD_SOURCE_FIELDS,
                ExtractNewRecordStateConfigDefinition.HANDLE_DELETES,
                ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES,
                ExtractNewRecordStateConfigDefinition.ADD_HEADERS,
                ExtractNewRecordStateConfigDefinition.ADD_FIELDS,
                SANITIZE_FIELD_NAMES);

        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        converter = new MongoDataConverter(
                ArrayEncoding.parse(config.getString(ARRAY_ENCODING)),
                FieldNameSelector.defaultNonRelationalSelector(config.getBoolean(SANITIZE_FIELD_NAMES)));

        addOperationHeader = config.getBoolean(OPERATION_HEADER);

        addSourceFields = determineAdditionalSourceField(config.getString(ADD_SOURCE_FIELDS));

        additionalHeaders = FieldReference.fromConfiguration(config.getString(ExtractNewRecordStateConfigDefinition.ADD_HEADERS));
        additionalFields = FieldReference.fromConfiguration(config.getString(ExtractNewRecordStateConfigDefinition.ADD_FIELDS));

        flattenStruct = config.getBoolean(FLATTEN_STRUCT);
        delimiter = config.getString(DELIMITER);

        dropTombstones = config.getBoolean(ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES);
        handleDeletes = DeleteHandling.parse(config.getString(ExtractNewRecordStateConfigDefinition.HANDLE_DELETES));

        final Map<String, String> afterExtractorConfig = new HashMap<>();
        afterExtractorConfig.put("field", "after");
        final Map<String, String> patchExtractorConfig = new HashMap<>();
        patchExtractorConfig.put("field", "patch");
        final Map<String, String> keyExtractorConfig = new HashMap<>();
        keyExtractorConfig.put("field", "id");

        afterExtractor.configure(afterExtractorConfig);
        patchExtractor.configure(patchExtractorConfig);
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
                throw new IllegalArgumentException("Unexpected field value: " + field);
            }
        }

        /**
         * Determine the struct hosting the given unqualified field.
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
