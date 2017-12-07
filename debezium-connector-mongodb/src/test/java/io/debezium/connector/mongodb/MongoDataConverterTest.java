/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Test;

import io.debezium.connector.mongodb.transforms.MongoDataConverter;

public class MongoDataConverterTest {

    public MongoDataConverterTest() throws IOException {
}
    String record = getFile("restaurants6.json");
    BsonDocument val = BsonDocument.parse(record);
    SchemaBuilder builder = SchemaBuilder.struct();

    @Test
    public void testConvertRecord() {

        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.addFieldSchema(entry, builder);
        }

        Schema finalSchema = builder.build();
        Struct struct = new Struct(finalSchema);

        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.convertRecord(entry, finalSchema, struct);
        }
        Assert.assertEquals(struct.toString(), "Struct{address=Struct{building=1007,coord=[-73.856077, 40.848447],street=Morris Park Ave,zipcode=10462},borough=Bronx,cuisine=Bakery,grades=[Struct{date=1393804800000,grade=A,score=2}, Struct{date=1378857600000,grade=A,score=6}, Struct{date=1358985600000,grade=A,score=10}, Struct{date=1322006400000,grade=A,score=9}, Struct{date=1299715200000,grade=B,score=14}],name=Morris Park Bake Shop,restaurant_id=30075445}");
    }

    @Test
    public void testAddFieldSchema() {
        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.addFieldSchema(entry, builder);
        }
        Schema finalSchema = builder.build();
        Schema docArray = SchemaBuilder.struct().field("date", Schema.OPTIONAL_INT64_SCHEMA).field("grade", Schema.OPTIONAL_STRING_SCHEMA).field("score", Schema.OPTIONAL_INT32_SCHEMA).build();
        Assert.assertEquals(finalSchema, SchemaBuilder.struct().field("address", SchemaBuilder.struct().field("building", Schema.OPTIONAL_STRING_SCHEMA).field("coord", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).build()).field("street", Schema.OPTIONAL_STRING_SCHEMA).field("zipcode", Schema.OPTIONAL_STRING_SCHEMA).build())
                .field("borough", Schema.OPTIONAL_STRING_SCHEMA).field("cuisine", Schema.OPTIONAL_STRING_SCHEMA).field("grades", SchemaBuilder.array(docArray).build()).field("name", Schema.OPTIONAL_STRING_SCHEMA).field("restaurant_id", Schema.OPTIONAL_STRING_SCHEMA).build());
    }
    
    private String getFile(String fileName) {
        StringBuilder result = new StringBuilder("");
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                result.append(line).append("\n");
            }
            scanner.close();
        } catch (IOException e) {
               e.printStackTrace();
        }
        return result.toString();
      }
}
