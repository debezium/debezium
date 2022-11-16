/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import static org.assertj.core.api.Assertions.assertThat;

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

import io.debezium.doc.FixFor;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.JsonPayloadNullFieldBehavior;

/**
 * @author vjuranek
 */
public class JsonSchemaDataTest {
    private JsonSchemaData jsonSchemaData;
    private ObjectMapper mapper;
    private String record;

    @Before
    public void setup() throws Exception {
        jsonSchemaData = new JsonSchemaData();
        mapper = new ObjectMapper();
        record = getFile("json/restaurants5.json");
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
        assertThat(expectedException).hasMessage("Field is not a homogenous array (1 x 2.0), different number types (Schema{INT32} x Schema{FLOAT64})");
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
        jsonSchemaData = new JsonSchemaData(JsonPayloadNullFieldBehavior.OPTIONAL_BYTES);
        String json = "{\"heartbeat\": 1, \"email\": null}";
        JsonNode testNode = mapper.readTree(json);
        Schema payloadSchema = jsonSchemaData.toConnectSchema("payload", testNode);
        Field emailField = payloadSchema.field("email");
        assertThat(emailField).isNotNull();
        assertThat(emailField.schema().type()).isEqualTo(Schema.OPTIONAL_BYTES_SCHEMA.schema().type());
    }
}
