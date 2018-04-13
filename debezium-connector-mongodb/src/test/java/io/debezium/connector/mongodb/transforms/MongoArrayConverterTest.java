/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map.Entry;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@code MongoDataConverter} that verifies array types.
 *
 * @author Jiri Pechanec
 */
public class MongoArrayConverterTest {

    private static final String HETEROGENOUS_ARRAY =
            "{\n" +
            "    \"_id\": 1,\n" +
            "    \"a2\": [\n" +
            "        11,\n" +
            "        \"abc\"\n" +
            "    ]\n" +
            "}";

    private static final String HETEROGENOUS_DOCUMENT_IN_ARRAY =
            "{\n" +
            "    \"_id\": 1,\n" +
            "    \"a1\": [\n" +
            "        {\n" +
            "            \"a\": 1\n" +
            "        },\n" +
            "        {\n" +
            "            \"a\": \"c\"\n" +
            "        }\n" +
            "    ],\n" +
            "}";

    private static final String HOMOGENOUS_ARRAYS =
            "{\n" +
            "    \"_id\": 1,\n" +
            "    \"a1\": [\n" +
            "        {\n" +
            "            \"a\": 1\n" +
            "        },\n" +
            "        {\n" +
            "            \"b\": \"c\"\n" +
            "        }\n" +
            "    ],\n" +
            "    \"a2\": [\n" +
            "        \"11\",\n" +
            "        \"abc\"\n" +
            "    ],\n" +
            "    \"empty\": []\n" +
            "}";

    private SchemaBuilder builder;

    @Before
    public void setup() throws Exception {
        builder = SchemaBuilder.struct().name("array");
    }

    @Test(expected = ConnectException.class)
    public void shouldDetectHeterogenousArray() throws Exception {
        final BsonDocument val = BsonDocument.parse(HETEROGENOUS_ARRAY);
        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.addFieldSchema(entry, builder);
        }
    }

    @Test(expected = ConnectException.class)
    public void shouldDetectHeterogenousDocumentInArray() throws Exception {
        final BsonDocument val = BsonDocument.parse(HETEROGENOUS_DOCUMENT_IN_ARRAY);
        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.addFieldSchema(entry, builder);
        }
    }

    @Test
    public void shouldCreateSchemaForHomogenousArray() throws Exception {
        final BsonDocument val = BsonDocument.parse(HOMOGENOUS_ARRAYS);
        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.addFieldSchema(entry, builder);
        }
        final Schema finalSchema = builder.build();

        assertThat(finalSchema)
                .isEqualTo(
                        SchemaBuilder.struct().name("array")
                                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("a1", SchemaBuilder.array(SchemaBuilder.struct().name("array.a1").optional()
                                        .field("a", Schema.OPTIONAL_INT32_SCHEMA)
                                        .field("b", Schema.OPTIONAL_STRING_SCHEMA)
                                        .build()
                                 ).optional().build())
                                .field("a2", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                                .field("empty", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                        .build());
        }

    @Test
    public void shouldCreateStructForHomogenousArray() throws Exception {
        final BsonDocument val = BsonDocument.parse(HOMOGENOUS_ARRAYS);
        final SchemaBuilder builder = SchemaBuilder.struct().name("array");

        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.addFieldSchema(entry, builder);
        }

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);

        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.convertRecord(entry, finalSchema, struct);
        }

        assertThat(struct.toString()).isEqualTo(
                "Struct{" +
                        "_id=1," +
                        "a1=[" +
                            "Struct{a=1}, " +
                            "Struct{b=c}" +
                        "]," +
                        "a2=[11, abc]," +
                        "empty=[]}"
        );
    }
}
