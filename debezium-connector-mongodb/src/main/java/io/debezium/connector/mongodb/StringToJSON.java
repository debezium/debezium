/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.MongoDataConverter;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class StringToJSON<R extends ConnectRecord<R>> implements Transformation<R> {

private final Logger logger = LoggerFactory.getLogger(getClass());

@Override
public R apply(R r) {
SchemaBuilder schemabuilder = SchemaBuilder.struct();
BsonDocument value = BsonDocument.parse(r.value().toString());
Set<Entry<String, BsonValue>> keyValues = value.entrySet();

for (Entry<String, BsonValue> keyValuesforSchema : keyValues) {
MongoDataConverter.addFieldSchema(keyValuesforSchema, schemabuilder);
}
Schema finalSchema = schemabuilder.build();
Struct finalStruct = new Struct(finalSchema);

for (Entry<String, BsonValue> keyvalueforStruct : keyValues) {

MongoDataConverter.convertRecord(keyvalueforStruct, finalSchema, finalStruct);
}

return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), finalSchema, finalStruct,r.timestamp());
}

public ConfigDef config() {
return new ConfigDef();
}

public void close() {
}

public void configure(Map<String, ?> map) {
}
}