/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static io.debezium.connector.mongodb.TestHelper.lines;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

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

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState.ArrayEncoding;
import io.debezium.doc.FixFor;

/**
 * Unit test for {@code MongoDataConverter} that verifies array types.
 *
 * @author Jiri Pechanec
 */
public class MongoArrayConverterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoArrayConverterTest.class);

    private static final String HETEROGENOUS_ARRAY = lines(
            "{",
            "    \"_id\": 1,",
            "    \"a2\": [",
            "        11,",
            "        \"abc\"",
            "    ]",
            "}");

    private static final String EMPTY_ARRAY = lines(
            "{",
            "    \"_id\": 1,",
            "    \"f\": []",
            "}");

    private static final String HETEROGENOUS_DOCUMENT_IN_ARRAY = lines(
            "{",
            "    \"_id\": 1,",
            "    \"a1\": [",
            "        {",
            "            \"a\": 1",
            "        },",
            "        {",
            "            \"a\": \"c\"",
            "        }",
            "    ],",
            "}");

    private static final String HOMOGENOUS_ARRAYS = lines(
            "{",
            "  \"_id\": 1,",
            "  \"a1\": [",
            "      {",
            "          \"a\": 1",
            "      },",
            "      {",
            "          \"b\": \"c\"",
            "      }",
            "  ],",
            "  \"a2\": [",
            "      \"11\",",
            "      \"abc\"",
            "  ],",
            "  \"empty\": [],",
            "  \"additionalContacts\": [",
            "    {",
            "      \"firstName\": \"John\",",
            "      \"lastName\": \"Doe\",",
            "      \"comment\": null",
            "    },",
            "    {",
            "      \"firstName\": \"Jane\",",
            "      \"lastName\": \"Doe\",",
            "      \"comment\": \"A comment\"",
            "    }",
            "  ]",
            "}");

    private static final String NESTED_DOCUMENT = lines("{\n" +
            "  \"pipeline\": [\n" +
            "    {\n" +
            "      \"stageId\": 1,\n" +
            "      \"componentList\": [\n" +
            "        {\n" +
            "          \"componentId\": 1,\n" +
            "          \"action\": \"deploy\"\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}");

    private static final String NESTED_SUB_DOCUMENT = lines("{\n" +
            "  \"pipeline\": [\n" +
            "    {\n" +
            "      \"stageId\": 1,\n" +
            "      \"componentList\": [\n" +
            "        {\n" +
            "          \"componentId\": 1,\n" +
            "          \"action\": \"deploy\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"componentId\": 2,\n" +
            "          \"action\": \"deploy\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"stageId\": 2,\n" +
            "      \"componentList\": [\n" +
            "        {\n" +
            "          \"componentId\": 3,\n" +
            "          \"action\": \"deploy\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"componentId\": 4,\n" +
            "          \"action\": \"deploy\"\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}");

    private SchemaBuilder builder;

    @Before
    public void setup() throws Exception {
        builder = SchemaBuilder.struct().name("array");
    }

    @Test(expected = DebeziumException.class)
    public void shouldDetectHeterogenousArray() throws Exception {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
        final BsonDocument val = BsonDocument.parse(HETEROGENOUS_ARRAY);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);
    }

    @Test(expected = DebeziumException.class)
    public void shouldDetectHeterogenousDocumentInArray() throws Exception {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
        final BsonDocument val = BsonDocument.parse(HETEROGENOUS_DOCUMENT_IN_ARRAY);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);
    }

    @Test
    @FixFor("DBZ-6760")
    public void shouldCreateSchemaForHomogenousArray() throws Exception {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
        final BsonDocument val = BsonDocument.parse(HOMOGENOUS_ARRAYS);

        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();

        assertThat(finalSchema)
                .isEqualTo(
                        SchemaBuilder.struct().name("array")
                                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("a1", SchemaBuilder.array(SchemaBuilder.struct().name("array.a1").optional()
                                        .field("a", Schema.OPTIONAL_INT32_SCHEMA)
                                        .field("b", Schema.OPTIONAL_STRING_SCHEMA)
                                        .build()).optional().build())
                                .field("a2", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                                .field("empty", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                                .field("additionalContacts", SchemaBuilder.array(SchemaBuilder.struct().name("array.additionalContacts").optional()
                                        .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
                                        .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
                                        .field("comment", Schema.OPTIONAL_STRING_SCHEMA)
                                        .build()).optional().build())
                                .build());
    }

    @Test
    @FixFor("DBZ-6760")
    public void shouldCreateStructForHomogenousArray() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
        final BsonDocument val = BsonDocument.parse(HOMOGENOUS_ARRAYS);
        final SchemaBuilder builder = SchemaBuilder.struct().name("array");

        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        // @formatter:off
        assertThat(struct.toString()).isEqualTo(
                "Struct{" +
                        "_id=1," +
                        "a1=[" +
                            "Struct{a=1}, " +
                            "Struct{b=c}" +
                        "]," +
                        "a2=[11, abc]," +
                        "empty=[]," +
                        "additionalContacts=[" +
                            "Struct{firstName=John,lastName=Doe}, " +
                            "Struct{firstName=Jane,lastName=Doe,comment=A comment}" +
                        "]}");
        // @formatter:on
    }

    @Test
    public void shouldCreateSchemaForEmptyArrayEncodingArray() throws Exception {
        final BsonDocument val = BsonDocument.parse(EMPTY_ARRAY);

        final MongoDataConverter arrayConverter = new MongoDataConverter(ArrayEncoding.ARRAY);
        Map<String, Map<Object, BsonType>> entry = arrayConverter.parseBsonDocument(val);
        arrayConverter.buildSchema(entry, builder);

        final Schema arraySchema = builder.build();

        assertThat(arraySchema)
                .isEqualTo(
                        SchemaBuilder.struct().name("array")
                                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("f", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                                .build());
    }

    @Test
    public void shouldCreateStructForEmptyArrayEncodingArray() {
        final BsonDocument val = BsonDocument.parse(EMPTY_ARRAY);

        final MongoDataConverter arrayConverter = new MongoDataConverter(ArrayEncoding.ARRAY);
        Map<String, Map<Object, BsonType>> entry = arrayConverter.parseBsonDocument(val);
        arrayConverter.buildSchema(entry, builder);
        final Schema arraySchema = builder.build();

        final Struct struct = new Struct(arraySchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            arrayConverter.buildStruct(bsonValueEntry, arraySchema, struct);
        }

        // @formatter:off
        assertThat(struct.toString()).isEqualTo(
                "Struct{" +
                        "_id=1," +
                        "f=[]" +
                "}");
        // @formatter:on
    }

    @Test
    public void shouldCreateSchemaForEmptyArrayEncodingDocument() {
        final BsonDocument val = BsonDocument.parse(EMPTY_ARRAY);

        final MongoDataConverter documentConverter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        Map<String, Map<Object, BsonType>> entry = documentConverter.parseBsonDocument(val);
        documentConverter.buildSchema(entry, builder);

        final Schema documentSchema = builder.build();

        assertThat(documentSchema)
                .isEqualTo(
                        SchemaBuilder.struct().name("array")
                                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("f", SchemaBuilder.struct().name("array.f").optional().build())
                                .build());
    }

    @Test
    public void shouldCreateStructForEmptyArrayEncodingDocument() {
        final BsonDocument val = BsonDocument.parse(EMPTY_ARRAY);

        final MongoDataConverter documentConverter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        Map<String, Map<Object, BsonType>> entry = documentConverter.parseBsonDocument(val);
        documentConverter.buildSchema(entry, builder);

        final Schema documentSchema = builder.build();

        final Struct struct = new Struct(documentSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            documentConverter.buildStruct(bsonValueEntry, documentSchema, struct);
        }

        assertThat(struct.toString()).isEqualTo(
                "Struct{" +
                        "_id=1," +
                        "f=Struct{}}");
    }

    @Test
    public void shouldCreateSchemaForHeterogenousArray() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(HETEROGENOUS_ARRAY);

        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();

        assertThat(finalSchema)
                .isEqualTo(
                        SchemaBuilder.struct().name("array")
                                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("a2", SchemaBuilder.struct().name("array.a2").optional()
                                        .field("_0", Schema.OPTIONAL_INT32_SCHEMA)
                                        .field("_1", Schema.OPTIONAL_STRING_SCHEMA)
                                        .build())
                                .build());
    }

    @Test
    public void shouldCreateStructForHeterogenousArray() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(HETEROGENOUS_ARRAY);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();

        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        assertThat(struct.toString()).isEqualTo(
                "Struct{" +
                        "_id=1," +
                        "a2=Struct{_0=11,_1=abc}}");
    }

    @Test
    public void shouldCreateSchemaForHeterogenousDocumentInArray() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(HETEROGENOUS_DOCUMENT_IN_ARRAY);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();

        assertThat(finalSchema)
                .isEqualTo(
                        SchemaBuilder.struct().name("array")
                                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("a1", SchemaBuilder.struct().name("array.a1").optional()
                                        .field("_0", SchemaBuilder.struct().name("array.a1._0").optional()
                                                .field("a", Schema.OPTIONAL_INT32_SCHEMA)
                                                .build())
                                        .field("_1", SchemaBuilder.struct().name("array.a1._1").optional()
                                                .field("a", Schema.OPTIONAL_STRING_SCHEMA)
                                                .build())
                                        .build())
                                .build());
    }

    @Test
    public void shouldCreateStructForHeterogenousDocumentInArray() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(HETEROGENOUS_DOCUMENT_IN_ARRAY);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);
        final Schema finalSchema = builder.build();

        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        // @formatter:off
        assertThat(struct.toString()).isEqualTo(
                "Struct{" +
                        "_id=1," +
                        "a1=Struct{" +
                            "_0=Struct{a=1}," +
                            "_1=Struct{a=c}" +
                        "}" +
                "}");
        // @formatter:on
    }

    @Test
    public void shouldCreateSchemaForNestedDocumentForArrayEncoding() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
        final BsonDocument val = BsonDocument.parse(NESTED_DOCUMENT);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        final String expectedStruct = "Struct{" +
                "pipeline=[" +
                "Struct{" +
                "stageId=1," +
                "componentList=[" +
                "Struct{" +
                "componentId=1," +
                "action=deploy" +
                "}" +
                "]" +
                "}" +
                "]" +
                "}";

        assertThat(struct.toString()).isEqualTo(expectedStruct);
    }

    @Test
    public void shouldCreateSchemaForNestedDocumentForDocumentEncoding() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(NESTED_DOCUMENT);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();

        final Schema expectedSchema = SchemaBuilder.struct().name("array")
                .field("pipeline", SchemaBuilder.struct().name("array.pipeline").optional()
                        .field("_0", SchemaBuilder.struct().name("array.pipeline._0").optional()
                                .field("stageId", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("componentList", SchemaBuilder.struct().name("array.pipeline._0.componentList").optional()
                                        .field("_0", SchemaBuilder.struct().name("array.pipeline._0.componentList._0").optional()
                                                .field("componentId", Schema.OPTIONAL_INT32_SCHEMA)
                                                .field("action", Schema.OPTIONAL_STRING_SCHEMA)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        assertThat(finalSchema).isEqualTo(expectedSchema);

        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        final String expectedStruct = "Struct{pipeline=Struct{_0=Struct{stageId=1,componentList=Struct{_0=Struct{componentId=1,action=deploy}}}}}";
        assertThat(struct.toString()).isEqualTo(expectedStruct);
    }

    @Test
    public void shouldCreateSchemaForNestedSubDocumentForArrayEncoding() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
        final BsonDocument val = BsonDocument.parse(NESTED_SUB_DOCUMENT);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        assertThat(struct.toString()).isEqualTo(
                "Struct{pipeline=[Struct{stageId=1,componentList=[Struct{componentId=1,action=deploy}, Struct{componentId=2,action=deploy}]}, Struct{stageId=2,componentList=[Struct{componentId=3,action=deploy}, Struct{componentId=4,action=deploy}]}]}");
    }

    @Test
    public void shouldCreateSchemaForNestedSubDocumentForDocumentEncoding() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(NESTED_SUB_DOCUMENT);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        assertThat(struct.toString()).isEqualTo(
                "Struct{pipeline=Struct{_0=Struct{stageId=1,componentList=Struct{_0=Struct{componentId=1,action=deploy},_1=Struct{componentId=2,action=deploy}}},_1=Struct{stageId=2,componentList=Struct{_0=Struct{componentId=3,action=deploy},_1=Struct{componentId=4,action=deploy}}}}}");
    }
}
