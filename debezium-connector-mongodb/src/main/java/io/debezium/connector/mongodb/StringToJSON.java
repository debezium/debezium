/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Transformation;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final Logger logger = LoggerFactory.getLogger(getClass());
    final ExtractField<R> delegate = new ExtractField.Value<R>();
    final ExtractField<R> delegate1 = new ExtractField.Value<R>();

    @Override
    public R apply(R r) {
        SchemaBuilder schemabuilder = SchemaBuilder.struct();
        BsonDocument value = null;

        final R newRecord = delegate.apply(r);
        final R newRecord1 = delegate1.apply(r);

        if (newRecord.value() == null) {
            value = BsonDocument.parse(newRecord1.value().toString());
        } else {
            value = BsonDocument.parse(newRecord.value().toString());
        }

        Set<Entry<String, BsonValue>> keyValues = value.entrySet();

        for (Entry<String, BsonValue> keyValuesforSchema : keyValues) {
            MongoDataConverter.addFieldSchema(keyValuesforSchema, schemabuilder);
        }

        Schema finalSchema = schemabuilder.build();
        Struct finalStruct = new Struct(finalSchema);

        for (Entry<String, BsonValue> keyvalueforStruct : keyValues) {
            MongoDataConverter.convertRecord(keyvalueforStruct, finalSchema, finalStruct);
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
        final Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", "after");
        final Map<String, String> delegateConfig1 = new HashMap<>();
        delegateConfig1.put("field", "patch");
        delegate.configure(delegateConfig);
        delegate1.configure(delegateConfig1);
    }
}