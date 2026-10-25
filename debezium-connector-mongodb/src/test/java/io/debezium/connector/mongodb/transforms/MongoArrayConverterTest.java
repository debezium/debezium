/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static io.debezium.connector.mongodb.TestHelper.lines;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState.ArrayEncoding;
import io.debezium.doc.FixFor;

/**
 * Unit test for {@code MongoDataConverter} that verifies array types.
 *
 * @author Jiri Pechanec
 */
public class MongoArrayConverterTest {

    private static final String HETEROGENEOUS_ARRAY = lines("""
            {
              "_id": 1,
              "a2": [
                11,
                "abc"
              ]
            }""");

    private static final String EMPTY_ARRAY = lines("""
            {
              "_id": 1,
              "f": []
            }""");

    private static final String HETEROGENEOUS_DOCUMENT_IN_ARRAY = lines("""
            {
              "_id": 1,
              "a1": [
                {
                  "a": 1
                },
                {
                  "a": "c"
                }
              ]
            }""");

    private static final String HOMOGENOUS_ARRAYS = lines("""
            {
              "_id": 1,
              "a1": [
                {
                  "a": 1
                },
                {
                  "b": "c"
                }
              ],
              "a2": [
                "11",
                "abc"
              ],
              "empty": [],
              "additionalContacts": [
                {
                  "firstName": "John",
                  "lastName": "Doe",
                  "comment": null
                },
                {
                  "firstName": "Jane",
                  "lastName": "Doe",
                  "comment": "A comment"
                }
              ]
            }""");

    private static final String NESTED_DOCUMENT = lines("""
            {
              "pipeline": [
                {
                  "stageId": 1,
                  "componentList": [
                    {
                      "componentId": 1,
                      "action": "deploy"
                    }
                  ]
                }
              ]
            }""");

    private static final String NESTED_SUB_DOCUMENT = lines("""
            {
              "pipeline": [
                {
                  "stageId": 1,
                  "componentList": [
                    {
                      "componentId": 1,
                      "action": "deploy"
                    },
                    {
                      "componentId": 2,
                      "action": "deploy"
                    }
                  ]
                },
                {
                  "stageId": 2,
                  "componentList": [
                    {
                      "componentId": 3,
                      "action": "deploy"
                    },
                    {
                      "componentId": 4,
                      "action": "deploy"
                    }
                  ]
                }
              ]
            }""");

    private static final String REGULAR_EXPRESSION_ARRAY = lines("""
            {
              "patterns": [
                { "$regularExpression": { "pattern": "^foo", "options": "i" } },
                { "$regularExpression": { "pattern": "bar$", "options": "m" } }
              ]
            }""");

    private static final String NESTED_REGULAR_EXPRESSION_ARRAY = lines("""
            {
              "patterns": [
                [
                  { "$regularExpression": { "pattern": "^foo", "options": "i" } },
                  { "$regularExpression": { "pattern": "bar$", "options": "m" } }
                ],
                [
                  { "$regularExpression": { "pattern": "", "options": "" } }
                ]
              ]
            }""");

    private SchemaBuilder builder;

    @BeforeEach
    void setup() throws Exception {
        builder = SchemaBuilder.struct().name("array");
    }

    @Test
    @FixFor("debezium/dbz#2562")
    void shouldCreateArrayOfRegularExpressions() {
        final var converter = new MongoDataConverter(ArrayEncoding.ARRAY);
        final var value = BsonDocument.parse(REGULAR_EXPRESSION_ARRAY);

        final Map<String, Map<Object, BsonType>> schemaMap = converter.parseBsonDocument(value);
        converter.buildSchema(schemaMap, builder);

        final var schema = builder.build();
        final var struct = new Struct(schema);
        for (final Map.Entry<String, BsonValue> entry : value.entrySet()) {
            converter.buildStruct(entry, schema, struct);
        }

        final var arraySchema = schema.field("patterns").schema();
        assertThat(arraySchema.type()).isEqualTo(Schema.Type.ARRAY);
        assertThat(arraySchema.valueSchema().name()).isEqualTo(MongoDataConverter.SCHEMA_NAME_REGEX);
        assertThat(arraySchema.valueSchema().fields()).extracting("name").containsExactly("regex", "options");

        final var patterns = struct.getArray("patterns");
        assertThat(patterns).hasSize(2);
        assertThat(patterns).allMatch(Struct.class::isInstance);

        final var firstPattern = (Struct) patterns.get(0);
        assertThat(firstPattern.getString("regex")).isEqualTo("^foo");
        assertThat(firstPattern.getString("options")).isEqualTo("i");

        final var secondPattern = (Struct) patterns.get(1);
        assertThat(secondPattern.getString("regex")).isEqualTo("bar$");
        assertThat(secondPattern.getString("options")).isEqualTo("m");
    }

    @Test
    @FixFor("debezium/dbz#2562")
    void shouldCreateDocumentEncodedArrayOfRegularExpressions() {
        final var converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final var value = BsonDocument.parse(REGULAR_EXPRESSION_ARRAY);

        final Map<String, Map<Object, BsonType>> schemaMap = converter.parseBsonDocument(value);
        converter.buildSchema(schemaMap, builder);

        final var schema = builder.build();
        final var struct = new Struct(schema);
        for (final Map.Entry<String, BsonValue> entry : value.entrySet()) {
            converter.buildStruct(entry, schema, struct);
        }

        final var patterns = struct.getStruct("patterns");
        final var firstPattern = patterns.getStruct("_0");
        assertThat(firstPattern.getString("regex")).isEqualTo("^foo");
        assertThat(firstPattern.getString("options")).isEqualTo("i");

        final var secondPattern = patterns.getStruct("_1");
        assertThat(secondPattern.getString("regex")).isEqualTo("bar$");
        assertThat(secondPattern.getString("options")).isEqualTo("m");
    }

    @ParameterizedTest
    @EnumSource(ArrayEncoding.class)
    @FixFor("debezium/dbz#2562")
    void shouldPreserveNestedRegularExpressions(ArrayEncoding encoding) {
        final var result = convertDocument(BsonDocument.parse(NESTED_REGULAR_EXPRESSION_ARRAY), encoding);
        assertNestedRegularExpressions(result.get("patterns"), encoding);
    }

    @ParameterizedTest
    @EnumSource(ArrayEncoding.class)
    @FixFor("debezium/dbz#2562")
    void shouldPreserveNestedRegularExpressionsInDocuments(ArrayEncoding encoding) {
        final var input = new BsonDocument("items", new BsonArray(List.of(BsonDocument.parse(NESTED_REGULAR_EXPRESSION_ARRAY))));
        final var result = convertDocument(input, encoding);
        final List<?> items = arrayElements(result.get("items"), encoding);
        assertThat(items).hasSize(1);
        assertNestedRegularExpressions(((Struct) items.get(0)).get("patterns"), encoding);
    }

    @ParameterizedTest
    @EnumSource(ArrayEncoding.class)
    @FixFor("debezium/dbz#2562")
    void shouldPreserveRepeatedRegularExpressionsAndSpecialPatterns(ArrayEncoding encoding) {
        final var regex = new BsonRegularExpression("\\d+\\s+한글", "im");
        final var input = new BsonDocument("patterns", new BsonArray(List.of(regex, regex, new BsonRegularExpression("", ""))));
        final var result = convertDocument(input, encoding);
        final List<?> patterns = arrayElements(result.get("patterns"), encoding);
        assertThat(patterns).hasSize(3);
        assertRegularExpression(patterns.get(0), "\\d+\\s+한글", "im");
        assertRegularExpression(patterns.get(1), "\\d+\\s+한글", "im");
        assertRegularExpression(patterns.get(2), "", "");
    }

    @ParameterizedTest
    @EnumSource(ArrayEncoding.class)
    @FixFor("debezium/dbz#2562")
    void shouldHandleMixedRegularExpressionArraysAccordingToEncoding(ArrayEncoding encoding) {
        for (final BsonValue other : List.of(new BsonString("plain"), BsonNull.VALUE)) {
            final var input = new BsonDocument("patterns", new BsonArray(List.of(new BsonRegularExpression("^foo", "i"), other)));
            if (encoding == ArrayEncoding.ARRAY) {
                assertThrows(DebeziumException.class, () -> convertDocument(input, encoding));
            }
            else {
                final var result = convertDocument(input, encoding);
                final List<?> patterns = arrayElements(result.get("patterns"), encoding);
                assertThat(patterns).hasSize(2);
                assertRegularExpression(patterns.get(0), "^foo", "i");
                assertThat(patterns.get(1)).isEqualTo(other.isNull() ? null : "plain");
            }
        }
    }

    private Struct convertDocument(BsonDocument input, ArrayEncoding encoding) {
        final var converter = new MongoDataConverter(encoding);
        final var builder = SchemaBuilder.struct().name("array");
        converter.buildSchema(converter.parseBsonDocument(input), builder);
        final var schema = builder.build();
        final var result = new Struct(schema);
        for (final Map.Entry<String, BsonValue> entry : input.entrySet()) {
            converter.buildStruct(entry, schema, result);
        }
        result.validate();
        return result;
    }

    private static List<?> arrayElements(Object value, ArrayEncoding encoding) {
        if (encoding == ArrayEncoding.ARRAY) {
            assertThat(value).isInstanceOf(List.class);
            return (List<?>) value;
        }
        assertThat(value).isInstanceOf(Struct.class);
        final var document = (Struct) value;
        return document.schema().fields().stream().map(document::get).toList();
    }

    private static void assertNestedRegularExpressions(Object value, ArrayEncoding encoding) {
        final List<?> groups = arrayElements(value, encoding);
        assertThat(groups).hasSize(2);
        final List<?> first = arrayElements(groups.get(0), encoding);
        final List<?> second = arrayElements(groups.get(1), encoding);
        assertThat(first).hasSize(2);
        assertThat(second).hasSize(1);
        assertRegularExpression(first.get(0), "^foo", "i");
        assertRegularExpression(first.get(1), "bar$", "m");
        assertRegularExpression(second.get(0), "", "");
    }

    private static void assertRegularExpression(Object value, String pattern, String options) {
        assertThat(value).isInstanceOf(Struct.class);
        final var regex = (Struct) value;
        assertThat(regex.schema().name()).isEqualTo(MongoDataConverter.SCHEMA_NAME_REGEX);
        assertThat(regex.getString("regex")).isEqualTo(pattern);
        assertThat(regex.getString("options")).isEqualTo(options);
    }

    @Test
    void shouldDetectHeterogenousArray() throws Exception {
        assertThrows(DebeziumException.class, () -> {
            final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
            final BsonDocument val = BsonDocument.parse(HETEROGENEOUS_ARRAY);
            Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
            converter.buildSchema(entry, builder);
        });
    }

    @Test
    void shouldDetectHeterogenousDocumentInArray() throws Exception {
        assertThrows(DebeziumException.class, () -> {
            final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
            final BsonDocument val = BsonDocument.parse(HETEROGENEOUS_DOCUMENT_IN_ARRAY);
            Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
            converter.buildSchema(entry, builder);
        });
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
        String expectedStruct = """
                Struct{
                    _id=1,
                    a1=[
                        Struct{
                            a=1
                        },
                        Struct{
                            b=c
                        }
                    ],
                    a2=[
                        11,
                        abc
                    ],
                    empty=[],
                    additionalContacts=[
                        Struct{
                            firstName=John,
                            lastName=Doe
                        },
                        Struct{
                            firstName=Jane,
                            lastName=Doe,
                            comment=A comment
                        }
                    ]
                }""";

        assertThat(struct.toString()).isEqualToIgnoringWhitespace(expectedStruct);
        // @formatter:on
    }

    @Test
    void shouldCreateSchemaForEmptyArrayEncodingArray() throws Exception {
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
    void shouldCreateStructForEmptyArrayEncodingArray() {
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
        assertThat(struct.toString()).isEqualToIgnoringWhitespace("""
                Struct{
                    _id=1,
                    f=[]
                }""");
        // @formatter:on
    }

    @Test
    void shouldCreateSchemaForEmptyArrayEncodingDocument() {
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
    void shouldCreateStructForEmptyArrayEncodingDocument() {
        final BsonDocument val = BsonDocument.parse(EMPTY_ARRAY);

        final MongoDataConverter documentConverter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        Map<String, Map<Object, BsonType>> entry = documentConverter.parseBsonDocument(val);
        documentConverter.buildSchema(entry, builder);

        final Schema documentSchema = builder.build();

        final Struct struct = new Struct(documentSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            documentConverter.buildStruct(bsonValueEntry, documentSchema, struct);
        }

        assertThat(struct.toString()).isEqualToIgnoringWhitespace("""
                Struct{
                    _id=1,
                    f=Struct{}
                }""");

    }

    @Test
    void shouldCreateSchemaForHeterogeneousArray() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(HETEROGENEOUS_ARRAY);

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
    void shouldCreateStructForHeterogeneousArray() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(HETEROGENEOUS_ARRAY);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();

        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        assertThat(struct.toString()).isEqualToIgnoringWhitespace("""
                Struct{
                    _id=1,
                    a2=Struct{
                        _0=11,
                        _1=abc
                    }
                }""");
    }

    @Test
    void shouldCreateSchemaForHeterogeneousDocumentInArray() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(HETEROGENEOUS_DOCUMENT_IN_ARRAY);
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
    void shouldCreateStructForHeterogeneousDocumentInArray() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(HETEROGENEOUS_DOCUMENT_IN_ARRAY);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);
        final Schema finalSchema = builder.build();

        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        // @formatter:off
        String expectedStruct = """
                Struct{
                    _id=1,
                    a1=Struct{
                        _0=Struct{a=1},
                        _1=Struct{a=c}
                    }
                }""";

        assertThat(struct.toString()).isEqualToIgnoringWhitespace(expectedStruct);
        // @formatter:on
    }

    @Test
    void shouldCreateSchemaForNestedDocumentForArrayEncoding() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
        final BsonDocument val = BsonDocument.parse(NESTED_DOCUMENT);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        final String expectedStruct = """
                Struct{
                    pipeline=[
                        Struct{
                            stageId=1,
                            componentList=[
                                Struct{
                                    componentId=1,
                                    action=deploy
                                }
                            ]
                        }
                    ]
                }""";

        assertThat(struct.toString()).isEqualToIgnoringWhitespace(expectedStruct);
    }

    @Test
    void shouldCreateSchemaForNestedDocumentForDocumentEncoding() {
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

        final String expectedStruct = """
                Struct{
                    pipeline=Struct{
                        _0=Struct{
                            stageId=1,
                            componentList=Struct{
                                _0=Struct{
                                    componentId=1,
                                    action=deploy
                                }
                            }
                        }
                    }
                }""";

        assertThat(struct.toString()).isEqualToIgnoringWhitespace(expectedStruct);
    }

    @Test
    void shouldCreateSchemaForNestedSubDocumentForArrayEncoding() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
        final BsonDocument val = BsonDocument.parse(NESTED_SUB_DOCUMENT);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        String expectedStruct = """
                Struct{
                    pipeline=[
                        Struct{
                            stageId=1,
                            componentList=[
                                Struct{
                                    componentId=1,
                                    action=deploy
                                },
                                Struct{
                                    componentId=2,
                                    action=deploy
                                }
                            ]
                        },
                        Struct{
                            stageId=2,
                            componentList=[
                                Struct{
                                    componentId=3,
                                    action=deploy
                                },
                                Struct{
                                    componentId=4,
                                    action=deploy
                                }
                            ]
                        }
                    ]
                }""";

        assertThat(struct.toString()).isEqualToIgnoringWhitespace(expectedStruct);
    }

    @Test
    void shouldCreateSchemaForNestedSubDocumentForDocumentEncoding() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse(NESTED_SUB_DOCUMENT);
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        String expectedStruct = """
                Struct{
                    pipeline=Struct{
                        _0=Struct{
                            stageId=1,
                            componentList=Struct{
                                _0=Struct{
                                    componentId=1,
                                    action=deploy
                                },
                                _1=Struct{
                                    componentId=2,
                                    action=deploy
                                }
                            }
                        },
                        _1=Struct{
                            stageId=2,
                            componentList=Struct{
                                _0=Struct{
                                    componentId=3,
                                    action=deploy
                                },
                                _1=Struct{
                                    componentId=4,
                                    action=deploy
                                }
                            }
                        }
                    }
                }""";

        assertThat(struct.toString()).isEqualToIgnoringWhitespace(expectedStruct);
    }

    @Test
    @FixFor("DBZ-9231")
    public void shouldProcessNestedDocumentsInArray() {
        final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
        final BsonDocument val = BsonDocument.parse("""
                { foo: [{ bar: { baz: 'whatever' } }] }
                """);
        builder = SchemaBuilder.struct().name("nestedArray");
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);
        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        String expectedStruct = """
                Struct{
                    foo=Struct{
                        _0=Struct{
                            bar=Struct{
                                baz=whatever
                            }
                        }
                    }
                }""";
        assertThat(struct.toString()).isEqualToIgnoringWhitespace(expectedStruct);
    }

    @Test
    public void shouldFailOnMixedArrayWithArrayEncoding() {
        assertThrows(DataException.class, () -> {
            final var converter = new MongoDataConverter(ArrayEncoding.ARRAY);
            final var val = BsonDocument.parse("""
                    {
                      "_id": 1,
                      "items": [1, {"name": "doc"}, 2]
                    }
                    """);
            builder = SchemaBuilder.struct().name("mixedArray");
            Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
            converter.buildSchema(entry, builder);
            final var finalSchema = builder.build();
            final var struct = new Struct(finalSchema);
            for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
                converter.buildStruct(bsonValueEntry, finalSchema, struct);
            }
        });
    }
}
