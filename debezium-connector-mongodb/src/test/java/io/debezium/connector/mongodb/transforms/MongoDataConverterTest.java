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
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState.ArrayEncoding;
import io.debezium.doc.FixFor;

/**
 * Unit test for {@code MongoDataConverter}.
 *
 * @author Sairam Polavarapu
 */
public class MongoDataConverterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDataConverterTest.class);
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
