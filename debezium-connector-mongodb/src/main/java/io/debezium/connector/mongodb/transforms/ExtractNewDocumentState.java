/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.CONFIG_FIELDS;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DELETED_FIELD;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Flatten;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.FieldNameAdjustmentMode;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.MongoDbFieldName;
import io.debezium.data.Envelope;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.transforms.AbstractExtractNewRecordState;

/**
 * Debezium Mongo Connector generates the CDC records in String format. Sink connectors usually are not able to parse
 * the string and insert the document as it is represented in the Source. so a user use this SMT to parse the String
 * and insert the MongoDB document in the JSON format.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Sairam Polavarapu
 * @author Renato mefi
 */
public class ExtractNewDocumentState<R extends ConnectRecord<R>> extends AbstractExtractNewRecordState<R> {

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

    private final ExtractField<R> keyExtractor = new ExtractField.Key<>();
    private final Flatten<R> recordFlattener = new Flatten.Value<>();
    private MongoDataConverter converter;
    private boolean flattenStruct;
    private String delimiter;

    private final Field.Set configFields = CONFIG_FIELDS.with(ARRAY_ENCODING, FLATTEN_STRUCT, DELIMITER);

    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);

        FieldNameAdjustmentMode fieldNameAdjustmentMode = FieldNameAdjustmentMode.parse(
                config.getString(CommonConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE));
        SchemaNameAdjuster fieldNameAdjuster = fieldNameAdjustmentMode.createAdjuster();
        converter = new MongoDataConverter(
                ArrayEncoding.parse(config.getString(ARRAY_ENCODING)),
                FieldNameSelector.defaultNonRelationalSelector(fieldNameAdjuster),
                fieldNameAdjustmentMode != FieldNameAdjustmentMode.NONE);

        flattenStruct = config.getBoolean(FLATTEN_STRUCT);
        delimiter = config.getString(DELIMITER);

        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", "id");
        keyExtractor.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("delimiter", delimiter);
        recordFlattener.configure(delegateConfig);
    }

    @Override
    public R doApply(R record) {
        if (!smtManager.isValidKey(record)) {
            return record;
        }
        // Add headers if needed
        if (!additionalHeaders.isEmpty()) {
            Headers headersToAdd = makeHeaders(additionalHeaders, (Struct) record.value());
            headersToAdd.forEach(h -> record.headers().add(h));
        }

        final R keyRecord = keyExtractor.apply(record);

        BsonDocument keyDocument = BsonDocument.parse("{ \"id\" : " + keyRecord.key().toString() + "}");
        BsonDocument valueDocument = new BsonDocument();

        // Tombstone message
        if (record.value() == null) {
            R newRecord = extractRecordStrategy.handleTruncateRecord(record);
            if (newRecord == null) {
                return null;
            }
            return newRecord(record, keyDocument, valueDocument);
        }

        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        final R afterRecord = extractRecordStrategy.afterDelegate().apply(record);
        final R updateDescriptionRecord = extractRecordStrategy.updateDescriptionDelegate().apply(record);
        boolean isDeletion = false;
        R newRecord;
        if (afterRecord.value() == null && updateDescriptionRecord.value() == null) {
            // Handling delete records
            isDeletion = true;
            newRecord = extractRecordStrategy.handleDeleteRecord(record);
            if (newRecord == null) {
                return null;
            }
        }
        else {
            // Handling insert and update records
            newRecord = extractRecordStrategy.handleRecord(record);
        }

        // insert || replace || update with capture.mode="change_streams_update_full" or "change_streams_update_full_with_pre_image"
        if (newRecord.value() != null) {
            valueDocument = getFullDocument(newRecord, keyDocument);
        }

        // update
        if (newRecord.value() == null && updateDescriptionRecord.value() != null) {
            valueDocument = getPartialUpdateDocument(newRecord, updateDescriptionRecord, keyDocument);
        }

        // add rewrite field
        if (extractRecordStrategy.isRewriteMode()) {
            valueDocument.append(DELETED_FIELD, new BsonBoolean(isDeletion));
        }

        return newRecord(record, keyDocument, valueDocument);
    }

    @Override
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
        super.close();
        keyExtractor.close();
        recordFlattener.close();
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

        if (!valueDocument.isEmpty()) {
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
            Optional<Schema> fieldSchema = fieldReference.getSchema(sourceSchema);
            fieldSchema.ifPresent(schema -> valueSchemaBuilder.field(fieldReference.getNewField(), schema));
        }
    }

    private void addFields(List<FieldReference> additionalFields, R originalRecord, Struct value) {
        Struct originalRecordValue = (Struct) originalRecord.value();

        // Update the value with the new fields
        for (FieldReference fieldReference : additionalFields) {
            value.put(fieldReference.getNewField(), fieldReference.getValue(originalRecordValue));
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
}
