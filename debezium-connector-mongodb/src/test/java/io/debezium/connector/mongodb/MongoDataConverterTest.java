/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Test;

public class MongoDataConverterTest {

    String record = "{ \"_id\" : 1 }";
    BsonDocument val = BsonDocument.parse(record);
    Set<Entry<String, BsonValue>> entry = val.entrySet();
    SchemaBuilder builder = SchemaBuilder.struct();

    @Test
    public void testConvertRecord() {

        for (Entry<String, BsonValue> entry2 : entry) {
            MongoDataConverter.addFieldSchema(entry2, builder);
        }
        
        Schema finalSchema = builder.build();
        Struct struct = new Struct(finalSchema);

        for (Entry<String, BsonValue> entry5 : entry) {
            MongoDataConverter.convertRecord(entry5, finalSchema, struct);
        }
        Assert.assertEquals(struct, new Struct(finalSchema).put("_id", 1));
        }

    @Test
    public void testAddFieldSchema() {
        for (Entry<String, BsonValue> entry2 : entry) {
            MongoDataConverter.addFieldSchema(entry2, builder);
        }
        Schema finalSchema = builder.build();
        Assert.assertEquals(finalSchema, SchemaBuilder.struct().field("_id", Schema.INT32_SCHEMA).build());
        }
}
