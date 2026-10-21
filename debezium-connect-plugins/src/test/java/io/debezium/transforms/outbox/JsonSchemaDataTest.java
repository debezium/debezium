/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig.FieldNameAdjustmentMode;
import io.debezium.doc.FixFor;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.FieldNameSelector.FieldNamer;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.JsonPayloadEmptyArrayBehavior;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.JsonPayloadNullFieldBehavior;

/**
 * @author vjuranek
 */
public class JsonSchemaDataTest {
    private JsonSchemaData jsonSchemaData;
    private ObjectMapper mapper;
    private String record;
    private FieldNamer<String> avroFieldNamer;

    @BeforeEach
    public void setup() throws Exception {
        jsonSchemaData = new JsonSchemaData();
        mapper = new ObjectMapper();
        record = getFile("json/restaurants5.json");
        avroFieldNamer = FieldNameSelector.defaultNonRelationalSelector(
                FieldNameAdjustmentMode.parse("avro").createAdjuster());
    }

    @Test
    @FixFor({ "DBZ-6910", "DBZ-6983" })
    public void shouldCreateCorrectSchemaFromArrayJson() throws Exception {
        String key = "test_arr";
        // remove description value from array json
        JsonNode testNode = mapper
                .readTree("[{\"code\":\"100\",\"description\":\"some description\"},{\"code\":\"200\",\"description\":\"another description\"},{\"code\":\"300\"}]");
        Schema arraySchema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(arraySchema.valueSchema().field("code")).isNotNull();
        assertThat(arraySchema.valueSchema().field("description").schema().type()).isEqualTo(Schema.Type.STRING);

        // set null value for description
        testNode = mapper.readTree("[{\"code\":\"100\",\"description\": null},{\"code\":\"200\",\"description\":\"another description\"},{\"code\":\"300\"}]");
        arraySchema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(arraySchema.valueSchema().field("code")).isNotNull();
        assertThat(arraySchema.valueSchema().field("description").schema().type()).isEqualTo(Schema.Type.STRING);

        // treat null value as bytes type
        jsonSchemaData = new JsonSchemaData(JsonPayloadNullFieldBehavior.OPTIONAL_BYTES, avroFieldNamer);
        testNode = mapper.readTree("[{\"code\":\"100\",\"description\": null},{\"code\":\"200\",\"description\": null},{\"code\":\"300\"}]");
        arraySchema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(arraySchema.valueSchema().field("code")).isNotNull();
        assertThat(arraySchema.valueSchema().field("description").schema().type()).isEqualTo(Schema.Type.BYTES);

        testNode = mapper.readTree("[{\"code\":\"100\",\"description\": null},{\"code\":\"200\",\"description\": \"another description\"},{\"code\":\"300\"}]");
        arraySchema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(arraySchema.valueSchema().field("code")).isNotNull();
        assertThat(arraySchema.valueSchema().field("description").schema().type()).isEqualTo(Schema.Type.STRING);

        // adjust the property name with non-avro supported character
        testNode = mapper.readTree(
                "[{\"code\":\"100\",\"description\":\"some description\", \"test$\":\"test\"},{\"code\":\"200\",\"description\":\"another description\"},{\"code\":\"300\"}]");
        arraySchema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(arraySchema.valueSchema().field("test_")).isNotNull();
    }

    @Test
    @FixFor("DBZ-5475")
    public void failSchemaCheckForArrayWithDifferentNumberTypes() throws Exception {
        JsonNode testNode = mapper.readTree("{\"test\": [1, 2.0, 3.0]}");

        RuntimeException expectedException = null;
        try {
            jsonSchemaData.toConnectSchema(null, testNode);
        }
        catch (ConnectException e) {
            expectedException = e;
        }
        assertThat(expectedException).isNotNull();
        assertThat(expectedException).isInstanceOf(ConnectException.class);
        assertThat(expectedException).hasMessage("Array elements have conflicting types (INT32 x FLOAT64).");
    }

    @Test
    @FixFor("DBZ-5654")
    public void shouldCreateCorrectSchemaFromInsertJson() throws Exception {
        JsonNode recordNode = mapper.readTree(record);
        Schema schema = jsonSchemaData.toConnectSchema("pub", recordNode);
        assertThat(schema).isEqualTo(
                SchemaBuilder.struct().name("pub").optional()
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
                                .field("date", SchemaBuilder.struct().name("pub.grades.date").optional()
                                        .field("$date", Schema.OPTIONAL_INT64_SCHEMA)
                                        .build())
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
    @FixFor("DBZ-5796")
    public void shouldConvertNullNodeToOptionalBytes() throws Exception {
        jsonSchemaData = new JsonSchemaData(JsonPayloadNullFieldBehavior.OPTIONAL_BYTES, avroFieldNamer);
        String json = "{\"heartbeat\": 1, \"email\": null}";
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        Field emailField = payloadSchema.field("email");
        assertThat(emailField).isNotNull();
        assertThat(emailField.schema().type()).isEqualTo(Schema.OPTIONAL_BYTES_SCHEMA.schema().type());
    }

    @Test
    @FixFor("debezium/dbz#921")
    public void shouldDropEmptyArrayFieldByDefault() throws Exception {
        String json = "{\"heartbeat\": 1, \"tags\": []}";
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        assertThat(payloadSchema.field("heartbeat")).isNotNull();
        assertThat(payloadSchema.field("tags")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#921")
    public void shouldKeepEmptyArrayFieldAsOptionalStringArray() throws Exception {
        jsonSchemaData = new JsonSchemaData(JsonPayloadNullFieldBehavior.IGNORE,
                JsonPayloadEmptyArrayBehavior.OPTIONAL_STRING, avroFieldNamer);
        String json = "{\"heartbeat\": 1, \"tags\": []}";
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        assertThat(payloadSchema.field("heartbeat")).isNotNull();
        Field tagsField = payloadSchema.field("tags");
        assertThat(tagsField).isNotNull();
        assertThat(tagsField.schema().type()).isEqualTo(Schema.Type.ARRAY);
        assertThat(tagsField.schema().valueSchema().type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    @FixFor("debezium/dbz#921")
    public void shouldKeepNestedEmptyArrayFieldAsOptionalStringArray() throws Exception {
        jsonSchemaData = new JsonSchemaData(JsonPayloadNullFieldBehavior.IGNORE,
                JsonPayloadEmptyArrayBehavior.OPTIONAL_STRING, avroFieldNamer);
        String json = "{\"nested\": {\"tags\": []}}";
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        Schema nestedSchema = payloadSchema.field("nested").schema();
        Field tagsField = nestedSchema.field("tags");
        assertThat(tagsField).isNotNull();
        assertThat(tagsField.schema().type()).isEqualTo(Schema.Type.ARRAY);
        assertThat(tagsField.schema().valueSchema().type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    @FixFor("debezium/dbz#921")
    public void shouldConvertEmptyArrayFieldToEmptyList() throws Exception {
        jsonSchemaData = new JsonSchemaData(JsonPayloadNullFieldBehavior.IGNORE,
                JsonPayloadEmptyArrayBehavior.OPTIONAL_STRING, avroFieldNamer);
        String json = "{\"heartbeat\": 1, \"tags\": []}";
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        Struct payload = (Struct) jsonSchemaData.toConnectData(testNode, payloadSchema);
        assertThat(payload.get("tags")).isEqualTo(List.of());
    }

    @Test
    @FixFor("debezium/dbz#1332")
    public void shouldMergeFieldsOfObjectsInNestedArray() throws Exception {
        String json = """
                {
                  "array": [
                    { "subarray": [ { "code": "100", "description": "some description" } ] },
                    { "subarray": [ { "code": "200", "comment": "some comment" } ] }
                  ]
                }
                """;
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        Schema subarrayItemSchema = payloadSchema.field("array").schema().valueSchema()
                .field("subarray").schema().valueSchema();
        assertThat(subarrayItemSchema.field("code")).isNotNull();
        assertThat(subarrayItemSchema.field("description")).isNotNull();
        assertThat(subarrayItemSchema.field("comment")).isNotNull();

        Struct payload = (Struct) jsonSchemaData.toConnectData(testNode, payloadSchema);
        List<Struct> array = payload.getArray("array");
        List<Struct> firstSubarray = array.get(0).getArray("subarray");
        List<Struct> secondSubarray = array.get(1).getArray("subarray");
        assertThat(firstSubarray.get(0).getString("description")).isEqualTo("some description");
        assertThat(secondSubarray.get(0).getString("comment")).isEqualTo("some comment");
    }

    @Test
    @FixFor("debezium/dbz#1332")
    public void shouldMergeFieldsOfNestedObjectsInArray() throws Exception {
        String json = """
                {
                  "items": [
                    { "details": { "code": "100", "description": "some description" } },
                    { "details": { "code": "200", "comment": "some comment" } }
                  ]
                }
                """;
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        Schema detailsSchema = payloadSchema.field("items").schema().valueSchema().field("details").schema();
        assertThat(detailsSchema.field("code")).isNotNull();
        assertThat(detailsSchema.field("description")).isNotNull();
        assertThat(detailsSchema.field("comment")).isNotNull();

        Struct payload = (Struct) jsonSchemaData.toConnectData(testNode, payloadSchema);
        List<Struct> items = payload.getArray("items");
        assertThat(items.get(0).getStruct("details").getString("description")).isEqualTo("some description");
        assertThat(items.get(1).getStruct("details").getString("comment")).isEqualTo("some comment");
    }

    @Test
    @FixFor("debezium/dbz#1332")
    public void failSchemaCheckForArrayOfObjectsWithConflictingFieldTypes() throws Exception {
        String json = """
                {
                  "example": [
                    { "code": "300", "description": "a description" },
                    { "code": "200", "description": true }
                  ]
                }
                """;
        JsonNode testNode = mapper.readTree(json);
        assertThatThrownBy(() -> jsonSchemaData.toConnectSchema("payload", testNode))
                .isInstanceOf(ConnectException.class)
                .hasMessage("Field 'description' has conflicting types in array elements (STRING x BOOLEAN).");
    }

    @Test
    @FixFor("debezium/dbz#1332")
    public void shouldMergeFieldsOfObjectsInListOfLists() throws Exception {
        String json = """
                {
                  "matrix": [
                    [ { "id": 1 } ],
                    [ { "id": 2, "name": "z" } ]
                  ]
                }
                """;
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        Schema cellSchema = payloadSchema.field("matrix").schema().valueSchema().valueSchema();
        assertThat(cellSchema.field("id")).isNotNull();
        assertThat(cellSchema.field("name")).isNotNull();

        Struct payload = (Struct) jsonSchemaData.toConnectData(testNode, payloadSchema);
        List<List<Struct>> matrix = payload.getArray("matrix");
        assertThat(matrix.get(1).get(0).getString("name")).isEqualTo("z");
    }

    @Test
    @FixFor("debezium/dbz#1332")
    public void shouldKeepMergedObjectsOptional() throws Exception {
        String json = """
                {
                  "a": [
                    { "d": { "p": 1 } },
                    { "d": { "q": 2 } },
                    {}
                  ]
                }
                """;
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        Schema itemSchema = payloadSchema.field("a").schema().valueSchema();
        assertThat(itemSchema.isOptional()).isTrue();
        assertThat(itemSchema.field("d").schema().isOptional()).isTrue();

        Struct payload = (Struct) jsonSchemaData.toConnectData(testNode, payloadSchema);
        List<Struct> items = payload.getArray("a");
        assertThat(items.get(2).getStruct("d")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#1332")
    public void shouldMergeEmptyArrayPlaceholderWithTypedArray() throws Exception {
        jsonSchemaData = new JsonSchemaData(JsonPayloadNullFieldBehavior.IGNORE,
                JsonPayloadEmptyArrayBehavior.OPTIONAL_STRING, avroFieldNamer);
        String json = """
                {
                  "a": [
                    { "ids": [1] },
                    { "ids": [] }
                  ]
                }
                """;
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        Schema idsSchema = payloadSchema.field("a").schema().valueSchema().field("ids").schema();
        assertThat(idsSchema.valueSchema().type()).isEqualTo(Schema.Type.INT32);

        Struct payload = (Struct) jsonSchemaData.toConnectData(testNode, payloadSchema);
        List<Struct> items = payload.getArray("a");
        assertThat(items.get(0).getArray("ids")).isEqualTo(List.of(1));
        assertThat(items.get(1).getArray("ids")).isEqualTo(List.of());
    }

    @Test
    @FixFor("debezium/dbz#1332")
    public void shouldKeepFieldOrderOfMergedObjectsAsInDocument() throws Exception {
        String json = """
                {
                  "a": [
                    { "f1": 1 },
                    { "f2": 1 },
                    { "f3": 1 },
                    { "f4": 1 },
                    { "f5": 1 },
                    { "f6": 1 }
                  ]
                }
                """;
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        Schema itemSchema = payloadSchema.field("a").schema().valueSchema();
        assertThat(itemSchema.fields()).extracting(Field::name)
                .containsExactly("f1", "f2", "f3", "f4", "f5", "f6");
    }
}
