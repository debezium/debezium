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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig.FieldNameAdjustmentMode;
import io.debezium.doc.FixFor;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.FieldNameSelector.FieldNamer;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.JsonPayloadNullFieldBehavior;

/**
 * @author vjuranek
 */
public class JsonSchemaDataTest {
    private JsonSchemaData jsonSchemaData;
    private ObjectMapper mapper;
    private String record;
    private FieldNamer<String> avroFieldNamer;

    @Before
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
    @FixFor({ "DBZ-8693" })
    public void shouldCreateCorrectSchemaFromNestedArrayJson() throws Exception {
        String key = "test_obj";
        // remove description value from nested array json in root object
        JsonNode testNode = mapper
                .readTree(
                        "{\"array\":[{\"subarray\":[{\"code\":\"100\",\"description\":\"some description\"}]},{\"subarray\":[{\"code\":\"200\",\"description\":\"another description\"}]},{\"subarray\":[{\"code\":\"300\"}]}]}");
        Schema schema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("code")).isNotNull();
        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("description").schema().type())
                .isEqualTo(Schema.Type.STRING);

        // remove description value from nested array json
        testNode = mapper
                .readTree(
                        "[{\"subarray\":[{\"code\":\"100\",\"description\":\"some description\"}]},{\"subarray\":[{\"code\":\"200\",\"description\":\"another description\"}]},{\"subarray\":[{\"code\":\"300\"}]}]");
        schema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(schema.valueSchema().field("subarray").schema().valueSchema().field("code")).isNotNull();
        assertThat(schema.valueSchema().field("subarray").schema().valueSchema().field("description").schema().type()).isEqualTo(Schema.Type.STRING);

        // set null value for description
        testNode = mapper.readTree(
                "{\"array\":[{\"subarray\":[{\"code\":\"100\",\"description\":null}]},{\"subarray\":[{\"code\":\"200\",\"description\":\"another description\"}]},{\"subarray\":[{\"code\":\"300\"}]}]}");
        schema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("code")).isNotNull();
        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("description").schema().type())
                .isEqualTo(Schema.Type.STRING);

        // set sub-array with primitives
        testNode = mapper.readTree(
                "{\"array\":[{\"subarray\":[{\"code\":\"100\",\"description\":\"some description\",\"subcodes\":[0]}]},{\"subarray\":[{\"code\":\"200\",\"description\":\"another description\",\"subcodes\":[-1, null]}]},{\"subarray\":[{\"code\":\"300\"}]}]}");
        schema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("code")).isNotNull();
        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("description").schema().type())
                .isEqualTo(Schema.Type.STRING);
        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("subcodes").schema().valueSchema().type())
                .isEqualTo(Schema.Type.INT32);

        // treat null value as bytes type
        jsonSchemaData = new JsonSchemaData(JsonPayloadNullFieldBehavior.OPTIONAL_BYTES, avroFieldNamer);
        testNode = mapper.readTree(
                "{\"array\":[{\"subarray\":[{\"code\":\"100\",\"description\":null}]},{\"subarray\":[{\"code\":\"200\",\"description\":null}]},{\"subarray\":[{\"code\":\"300\"}]}]}");
        schema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("code")).isNotNull();
        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("description").schema().type())
                .isEqualTo(Schema.Type.BYTES);

        testNode = mapper.readTree(
                "{\"array\":[{\"subarray\":[{\"code\":\"100\",\"description\":null}]},{\"subarray\":[{\"code\":\"200\",\"description\":\"another description\"}]},{\"subarray\":[{\"code\":\"300\"}]}]}");
        schema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("code")).isNotNull();
        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("description").schema().type())
                .isEqualTo(Schema.Type.STRING);

        // adjust the property name with non-avro supported character
        testNode = mapper.readTree(
                "{\"array\":[{\"subarray\":[{\"code\":\"100\",\"description\":\"some description\",\"test$\":\"test\"}]},{\"subarray\":[{\"code\":\"200\",\"description\":\"another description\"}]},{\"subarray\":[{\"code\":\"300\"}]}]}");
        schema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("test_")).isNotNull();

        // adjust the property name with non-avro supported character
        testNode = mapper.readTree(
                "{\"array\":[{\"subarray\":[{\"code\":\"100\",\"description\":\"some description\",\"test$\":\"test\"}]},{\"subarray\":[{\"code\":\"200\",\"description\":\"another description\"}]},{\"subarray\":[{\"code\":\"300\"}]}]}");
        schema = jsonSchemaData.toConnectSchema(key, testNode);

        assertThat(schema.field("array").schema().valueSchema().field("subarray").schema().valueSchema().field("test_")).isNotNull();
    }

    @Test
    @FixFor({ "DBZ-5475", "DBZ-8693" })
    public void failSchemaCheckForArrayWithDifferentTypes() {
        String key = "test_obj";

        // have conflicting number types in the array
        assertThatThrownBy(() -> jsonSchemaData.toConnectSchema(key, mapper.readTree("{\"test\": [1, 2.0, 3.0]}")))
                .isInstanceOf(ConnectException.class)
                .hasMessageStartingWith("Schemas are of different types")
                .hasMessageContaining("INT32")
                .hasMessageContaining("FLOAT64");

        // have conflicting types in the array
        assertThatThrownBy(() -> jsonSchemaData.toConnectSchema(key, mapper
                .readTree("[1, \"some string\"]")))
                .isInstanceOf(ConnectException.class)
                .hasMessageStartingWith("Schemas are of different types")
                .hasMessageContaining("STRING")
                .hasMessageContaining("INT32");
        assertThatThrownBy(() -> jsonSchemaData.toConnectSchema(key, mapper
                .readTree("[{\"test\":[1]}, [1]]")))
                .isInstanceOf(ConnectException.class)
                .hasMessageStartingWith("Schemas are of different types")
                .hasMessageContaining("STRUCT")
                .hasMessageContaining("ARRAY");

        // have conflicting types in the matrix
        assertThatThrownBy(() -> jsonSchemaData.toConnectSchema(key, mapper
                .readTree("[[0],[\"some string\"]]")))
                .isInstanceOf(ConnectException.class)
                .hasMessageStartingWith("Schemas are of different types")
                .hasMessageContaining("STRING")
                .hasMessageContaining("INT32");

        // have conflicting types in the nested matrix
        assertThatThrownBy(() -> jsonSchemaData.toConnectSchema(key, mapper
                .readTree("{\"array\":[{\"subarray\":[[0],[1]]},{\"subarray\":[]},{\"subarray\":[[\"some string\"]]}]}")))
                .isInstanceOf(ConnectException.class)
                .hasMessageStartingWith("Schemas are of different types")
                .hasMessageContaining("STRING")
                .hasMessageContaining("INT32");

        // have conflicting struct types in the nested array
        assertThatThrownBy(() -> jsonSchemaData.toConnectSchema(key, mapper
                .readTree(
                        "{\"array\":[{\"subarray\":[{\"code\":\"100\",\"description\":\"some description\"}]},{\"subarray\":[{\"code\":\"200\",\"description\":true}]},{\"subarray\":[{\"code\":\"300\"}]}]}")))
                .isInstanceOf(ConnectException.class)
                .hasMessageStartingWith("Schemas are of different types")
                .hasMessageContaining("STRING")
                .hasMessageContaining("BOOLEAN");

        // set null value in subarray
        assertThatThrownBy(() -> jsonSchemaData.toConnectSchema(key, mapper
                .readTree(
                        "{\"array\":[{\"subarray\":[null, {\"code\":\"100\",\"description\":null}, null]},{\"subarray\":[null, {\"code\":\"200\",\"description\":\"another description\"}]},{\"subarray\":[{\"code\":\"300\"}, null]},{\"subarray\":[null]}]}")))
                .isInstanceOf(ConnectException.class)
                .hasMessage("Array '' has unrecognized member schema.");
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
}
