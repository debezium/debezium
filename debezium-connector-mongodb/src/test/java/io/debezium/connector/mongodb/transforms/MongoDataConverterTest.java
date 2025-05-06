/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState.ArrayEncoding;
import io.debezium.doc.FixFor;

/**
 * Unit test for {@code MongoDataConverter}.
 *
 * @author Sairam Polavarapu
 */
public class MongoDataConverterTest {

    private String record;
    private BsonDocument val;
    private SchemaBuilder builder;
    private MongoDataConverter converter;

    @Before
    public void setup() throws Exception {
        record = getFile("restaurants5.json");
        val = BsonDocument.parse(record);
        builder = SchemaBuilder.struct().name("pub");
        converter = new MongoDataConverter(ArrayEncoding.ARRAY);
    }

    @Test
    public void shouldCreateCorrectStructFromInsertJson() {
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        assertThat(struct.toString()).isEqualToIgnoringWhitespace("""
                Struct{
                    address=Struct{
                        building=1007,
                        floor=Struct{
                            level=17,
                            description=level 17
                        },
                        coord=[-73.856077, 40.848447],
                        street=Morris Park Ave,
                        zipcode=10462
                    },
                    borough=Bronx,
                    cuisine=Bakery,
                    grades=[
                        Struct{date=Mon Mar 03 00:00:00 UTC 2014,grade=A,score=2},
                        Struct{date=Wed Sep 11 00:00:00 UTC 2013,grade=A,score=6},
                        Struct{date=Thu Jan 24 00:00:00 UTC 2013,grade=A,score=10},
                        Struct{date=Wed Nov 23 00:00:00 UTC 2011,grade=A,score=9},
                        Struct{date=Thu Mar 10 00:00:00 UTC 2011,grade=B,score=14}
                    ],
                    name=Morris Park Bake Shop,
                    restaurant_id=30075445
                }""");
    }

    @Test
    public void shouldCreateCorrectSchemaFromInsertJson() {
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        assertThat(finalSchema).isEqualTo(SchemaBuilder.struct().name("pub")
                .field("address", SchemaBuilder.struct().name("pub.address").optional()
                        .field("building", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("floor", SchemaBuilder.struct().name("pub.address.floor").optional()
                                .field("level", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("description", Schema.OPTIONAL_STRING_SCHEMA)
                                .build())
                        .field("coord", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
                        .field("street", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("zipcode", Schema.OPTIONAL_STRING_SCHEMA)
                        .build())
                .field("borough", Schema.OPTIONAL_STRING_SCHEMA)
                .field("cuisine", Schema.OPTIONAL_STRING_SCHEMA)
                .field("grades", SchemaBuilder.array(SchemaBuilder.struct().name("pub.grades").optional()
                        .field("date", Timestamp.builder().optional().build())
                        .field("grade", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("score", Schema.OPTIONAL_INT32_SCHEMA)
                        .build())
                        .optional()
                        .build())
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("restaurant_id", Schema.OPTIONAL_STRING_SCHEMA)
                .build());
    }

    private String getFile(String fileName) throws IOException, URISyntaxException {
        URL jsonResource = getClass().getClassLoader().getResource(fileName);
        return new String(
                Files.readAllBytes(Paths.get(jsonResource.toURI())),
                StandardCharsets.UTF_8);
    }

    @Test
    @FixFor("DBZ-928")
    public void shouldProcessNullValue() {
        val = BsonDocument.parse("{\n" +
                "    \"_id\" : ObjectId(\"51e5619ee4b01f9fbdfba9fc\"),\n" +
                "    \"delivery\" : {\n" +
                "        \"hour\" : null,\n" +
                "        \"hourId\" : 10\n" +
                "    }\n" +
                "}");
        builder = SchemaBuilder.struct().name("withnull");
        converter = new MongoDataConverter(ArrayEncoding.ARRAY);

        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        assertThat(finalSchema).isEqualTo(
                SchemaBuilder.struct().name("withnull")
                        .field("_id", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("delivery", SchemaBuilder.struct().name("withnull.delivery").optional()
                                .field("hour", Schema.OPTIONAL_STRING_SCHEMA)
                                .field("hourId", Schema.OPTIONAL_INT32_SCHEMA)
                                .build())
                        .build());
        assertThat(struct.toString()).isEqualTo(
                "Struct{"
                        + "_id=51e5619ee4b01f9fbdfba9fc,"
                        + "delivery=Struct{"
                        + "hourId=10"
                        + "}"
                        + "}");
    }

    @Test
    @FixFor("DBZ-1315")
    public void shouldProcessUnsupportedValue() {
        val = BsonDocument.parse("{\n" +
                "    \"_id\" : ObjectId(\"518cc94bc27cfa20d9693e5d\"),\n" +
                "    \"name\" : undefined,\n" +
                "    \"address\" : {\n" +
                "        \"building\" : undefined,\n" +
                "        \"floor\" : 10\n" +
                "    }\n" +
                "}");
        builder = SchemaBuilder.struct().name("withundefined");
        converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);

        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }
        assertThat(finalSchema).isEqualTo(
                SchemaBuilder.struct().name("withundefined")
                        .field("_id", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("address", SchemaBuilder.struct().name("withundefined.address").optional()
                                .field("floor", Schema.OPTIONAL_INT32_SCHEMA)
                                .build())
                        .build());
        assertThat(struct.toString()).isEqualTo(
                "Struct{"
                        + "_id=518cc94bc27cfa20d9693e5d,"
                        + "address=Struct{"
                        + "floor=10"
                        + "}"
                        + "}");

    }
}
