/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Transformation;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * Debezium Mongo Connector generates the CDC records in String format. Sink connectors usually are not able to parse
 * the string and insert the document as it is represented in the Source. so a user use this SMT to parse the String 
 * and insert the MongoDB document in the JSON format..
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Sairam Polavarapu
 */
public class StringToJSON<R extends ConnectRecord<R>> implements Transformation<R> {

    final ExtractField<R> afterExtractor = new ExtractField.Value<R>();
    final ExtractField<R> patchExtractor = new ExtractField.Value<R>();

    @Override
    public R apply(R r) {
        SchemaBuilder schemabuilder = SchemaBuilder.struct();
        BsonDocument value = null;

        final R afterRecord = afterExtractor.apply(r);

        if (afterRecord.value() == null) {
            final R patchRecord = patchExtractor.apply(r);
            value = BsonDocument.parse(patchRecord.value().toString());
        } else {
            value = BsonDocument.parse(afterRecord.value().toString());
        }

        Set<Entry<String, BsonValue>> keyValues = value.entrySet();

        for (Entry<String, BsonValue> keyValuesforSchema : keyValues) {
            if(keyValuesforSchema.getKey().toString().equalsIgnoreCase("$set")) {
                BsonDocument val1 = BsonDocument.parse(keyValuesforSchema.getValue().toString());
                Set<Entry<String, BsonValue>> keyValuesforSetSchema = val1.entrySet();
                for (Entry<String, BsonValue> keyValuesforSetSchemaEntry : keyValuesforSetSchema) {
                    MongoDataConverter.addFieldSchema(keyValuesforSetSchemaEntry, schemabuilder);
                    }
                } else {
                    MongoDataConverter.addFieldSchema(keyValuesforSchema, schemabuilder);
                    }
        }

        Schema finalSchema = schemabuilder.build();
        Struct finalStruct = new Struct(finalSchema);

        for (Entry<String, BsonValue> keyvalueforStruct : keyValues) {
            if(keyvalueforStruct.getKey().toString().equalsIgnoreCase("$set")) {
                BsonDocument val1 = BsonDocument.parse(keyvalueforStruct.getValue().toString());
                Set<Entry<String, BsonValue>> keyvalueforSetStruct = val1.entrySet();
                for (Entry<String, BsonValue> keyvalueforSetStructEntry : keyvalueforSetStruct) {
                    MongoDataConverter.convertRecord(keyvalueforSetStructEntry, finalSchema, finalStruct);
                    }
                } else {
            MongoDataConverter.convertRecord(keyvalueforStruct, finalSchema, finalStruct);
            }
        }

        return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), finalSchema, finalStruct,
                r.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> map) {
        final Map<String, String> afterExtractorConfig = new HashMap<>();
        afterExtractorConfig.put("field", "after");
        final Map<String, String> patchExtractorConfig = new HashMap<>();
        patchExtractorConfig.put("field", "patch");
        afterExtractor.configure(afterExtractorConfig);
        patchExtractor.configure(patchExtractorConfig);
    }
}
