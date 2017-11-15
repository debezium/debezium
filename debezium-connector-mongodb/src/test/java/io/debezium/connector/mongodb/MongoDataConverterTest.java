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

    String record = "{\"address\": {\"building\": \"1007\", \"coord\": [-73.856077, 40.848447], \"street\": \"Morris Park Ave\", \"zipcode\": \"10462\"}, \"borough\": \"Bronx\", \"cuisine\": \"Bakery\", \"grades\": [{\"date\": {\"$date\": 1393804800000}, \"grade\": \"A\", \"score\": 2}, {\"date\": {\"$date\": 1378857600000}, \"grade\": \"A\", \"score\": 6}, {\"date\": {\"$date\": 1358985600000}, \"grade\": \"A\", \"score\": 10}, {\"date\": {\"$date\": 1322006400000}, \"grade\": \"A\", \"score\": 9}, {\"date\": {\"$date\": 1299715200000}, \"grade\": \"B\", \"score\": 14}], \"name\": \"Morris Park Bake Shop\", \"restaurant_id\": \"30075445\"}";
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
        Assert.assertEquals(struct.toString(), "Struct{address=Struct{building=1007,coord=[-73.856077, 40.848447],street=Morris Park Ave,zipcode=10462},borough=Bronx,cuisine=Bakery,grades=[Struct{date=1393804800000,grade=A,score=2}, Struct{date=1378857600000,grade=A,score=6}, Struct{date=1358985600000,grade=A,score=10}, Struct{date=1322006400000,grade=A,score=9}, Struct{date=1299715200000,grade=B,score=14}],name=Morris Park Bake Shop,restaurant_id=30075445}");
    }

    @Test
    public void testAddFieldSchema() {
        for (Entry<String, BsonValue> entry2 : entry) {
            MongoDataConverter.addFieldSchema(entry2, builder);
        }
        Schema finalSchema = builder.build();
        Schema docArray = SchemaBuilder.struct().field("date", Schema.INT64_SCHEMA).field("grade", Schema.STRING_SCHEMA).field("score", Schema.INT32_SCHEMA).build();
        Assert.assertEquals(finalSchema, SchemaBuilder.struct().field("address", SchemaBuilder.struct().field("building", Schema.STRING_SCHEMA).field("coord", SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build()).field("street", Schema.STRING_SCHEMA).field("zipcode", Schema.STRING_SCHEMA).build())
                .field("borough", Schema.STRING_SCHEMA).field("cuisine", Schema.STRING_SCHEMA).field("grades", SchemaBuilder.array(docArray).build()).field("name", Schema.STRING_SCHEMA).field("restaurant_id", Schema.STRING_SCHEMA).build());
    }
}
