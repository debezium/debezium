/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Transformation;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;

/**
 * Debezium Mongo Connector generates the CDC records in String format. Sink connectors usually are not able to parse
 * the string and insert the document as it is represented in the Source. so a user use this SMT to parse the String
 * and insert the MongoDB document in the JSON format.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Sairam Polavarapu
 */
public class UnwrapFromMongoDbEnvelope<R extends ConnectRecord<R>> implements Transformation<R> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final Field ARRAY_ENCODING = Field.create("array.encoding")
            .withDisplayName("Array encoding")
            .withEnum(ArrayEncoding.class, ArrayEncoding.ARRAY)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The arrays can be encoded using 'array' schema type (the default) ar as a 'struct' (similar to how BSON encodes arrays). "
                    + "'array' is easier to consume but requires all elemnts in the array to be of the same type. "
                    + "Use 'struct' if the arrays in data source mixes different types together.");

    public static enum ArrayEncoding implements EnumeratedValue {
        ARRAY("array"),
        DOCUMENT("document");

        private final String value;

        private ArrayEncoding(String value) {
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
            if (value == null) return null;
            value = value.trim();
            for (ArrayEncoding option : ArrayEncoding.values()) {
                if (option.getValue().equalsIgnoreCase(value)) return option;
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static ArrayEncoding parse(String value, String defaultValue) {
            ArrayEncoding mode = parse(value);
            if (mode == null && defaultValue != null) mode = parse(defaultValue);
            return mode;
        }
    }


    private final ExtractField<R> afterExtractor = new ExtractField.Value<R>();
    private final ExtractField<R> patchExtractor = new ExtractField.Value<R>();
    private final ExtractField<R> keyExtractor = new ExtractField.Key<R>();
    private MongoDataConverter converter;

    @Override
    public R apply(R r) {
        String newValueSchemaName = r.valueSchema().name();
        if (newValueSchemaName.endsWith(".Envelope")) {
            newValueSchemaName = newValueSchemaName.substring(0, newValueSchemaName.length() - 9);
        }
        SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().name(newValueSchemaName);
        SchemaBuilder keySchemabuilder = SchemaBuilder.struct();
        BsonDocument valueDocument = null;

        final R afterRecord = afterExtractor.apply(r);
        final R key = keyExtractor.apply(r);
        BsonDocument keyDocument = BsonDocument.parse("{ \"id\" : " + key.key().toString() + "}");

        if (afterRecord.value() == null) {
            final R patchRecord = patchExtractor.apply(r);

            // update
            if (patchRecord.value() != null) {
                valueDocument = BsonDocument.parse(patchRecord.value().toString());
                valueDocument = valueDocument.getDocument("$set");

                if (!valueDocument.containsKey("id")) {
                    valueDocument.append("id", keyDocument.get("id"));
                }
            }
            // delete
            else {
                valueDocument = new BsonDocument();
            }
        // insert
        }
        else {
            valueDocument = BsonDocument.parse(afterRecord.value().toString());
            valueDocument.remove("_id");
            valueDocument.append("id", keyDocument.get("id"));
        }

        Set<Entry<String, BsonValue>> valuePairs = valueDocument.entrySet();
        Set<Entry<String, BsonValue>> keyPairs = keyDocument.entrySet();

        for (Entry<String, BsonValue> valuePairsforSchema : valuePairs) {
            if(valuePairsforSchema.getKey().toString().equalsIgnoreCase("$set")) {
                BsonDocument val1 = BsonDocument.parse(valuePairsforSchema.getValue().toString());
                Set<Entry<String, BsonValue>> keyValuesforSetSchema = val1.entrySet();
                for (Entry<String, BsonValue> keyValuesforSetSchemaEntry : keyValuesforSetSchema) {
                    converter.addFieldSchema(keyValuesforSetSchemaEntry, valueSchemaBuilder);
                }
            } else {
                converter.addFieldSchema(valuePairsforSchema, valueSchemaBuilder);
            }
        }

        for (Entry<String, BsonValue> keyPairsforSchema : keyPairs) {
            converter.addFieldSchema(keyPairsforSchema, keySchemabuilder);
        }

        Schema finalValueSchema = valueSchemaBuilder.build();
        Struct finalValueStruct = new Struct(finalValueSchema);
        Schema finalKeySchema = keySchemabuilder.build();
        Struct finalKeyStruct = new Struct(finalKeySchema);

        for (Entry<String, BsonValue> valuePairsforStruct : valuePairs) {
            if(valuePairsforStruct.getKey().toString().equalsIgnoreCase("$set")) {
                BsonDocument val1 = BsonDocument.parse(valuePairsforStruct.getValue().toString());
                Set<Entry<String, BsonValue>> keyvalueforSetStruct = val1.entrySet();
                for (Entry<String, BsonValue> keyvalueforSetStructEntry : keyvalueforSetStruct) {
                    converter.convertRecord(keyvalueforSetStructEntry, finalValueSchema, finalValueStruct);
                }
            } else {
                converter.convertRecord(valuePairsforStruct, finalValueSchema, finalValueStruct);
            }
        }

        for (Entry<String, BsonValue> keyPairsforStruct : keyPairs) {
            converter.convertRecord(keyPairsforStruct, finalKeySchema, finalKeyStruct);
        }

        if (finalValueSchema.fields().isEmpty()) {
            return r.newRecord(r.topic(), r.kafkaPartition(), finalKeySchema, finalKeyStruct, null, null,
                    r.timestamp());
        }
        else {
            return r.newRecord(r.topic(), r.kafkaPartition(), finalKeySchema, finalKeyStruct, finalValueSchema, finalValueStruct,
                    r.timestamp());
        }
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, ARRAY_ENCODING);
        return config;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> map) {
        final Configuration config = Configuration.from(map);
        final Field.Set configFields = Field.setOf(ARRAY_ENCODING);
        if (!config.validateAndRecord(configFields, logger::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        converter = new MongoDataConverter(ArrayEncoding.parse(config.getString(ARRAY_ENCODING)));

        final Map<String, String> afterExtractorConfig = new HashMap<>();
        afterExtractorConfig.put("field", "after");
        final Map<String, String> patchExtractorConfig = new HashMap<>();
        patchExtractorConfig.put("field", "patch");
        final Map<String, String> keyExtractorConfig = new HashMap<>();
        keyExtractorConfig.put("field", "id");
        afterExtractor.configure(afterExtractorConfig);
        patchExtractor.configure(patchExtractorConfig);
        keyExtractor.configure(keyExtractorConfig);
    }
}
