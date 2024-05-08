/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

public class MongoDbConnectorConfigTest {

    @Test
    public void parseSignallingMessage() {
        Schema schema = new SchemaBuilder(Schema.Type.STRUCT).field("after", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(schema);
        struct.put("after", "{\"_id\":\"test-1\"," +
                "\"type\":\"execute-snapshot\"," +
                "\"data\":{\"data-collections\":[\"database.collection\"],\"type\":\"incremental\"}}");
        MongoDbConnectorConfig mongoDbConnectorConfig = new MongoDbConnectorConfig(TestHelper.getConfiguration());

        Optional<String[]> resultOpt = mongoDbConnectorConfig.parseSignallingMessage(struct);

        Assert.assertTrue(resultOpt.isPresent());
        String[] result = resultOpt.get();
        Assert.assertEquals(3, result.length);
        Assert.assertEquals("test-1", result[0]);
        Assert.assertEquals("execute-snapshot", result[1]);
        Assert.assertEquals("{\"data-collections\": [\"database.collection\"], \"type\": \"incremental\"}", result[2]);
    }

    @Test
    public void parseCursorPipeline() {
        verifyCursorPipelineValidateError("This is not valid JSON pipeline",
                "Change stream pipeline JSON is invalid: JSON reader was expecting a value but found 'This'.");
        verifyCursorPipelineValidateError("{$match: {}}", "Change stream pipeline JSON is invalid: Cannot cast org.bson.Document to java.util.List");

        verifyCursorPipelineValidateSuccess(null);
        verifyCursorPipelineValidateSuccess("");
        verifyCursorPipelineValidateSuccess("[]");
        verifyCursorPipelineValidateSuccess("[{$match: {}}]");
        verifyCursorPipelineValidateSuccess("[{\"$match\": { \"$and\": [{\"operationType\": \"insert\"}, {\"fullDocument.eventId\": 1404 }] } }]\n");
    }

    private static void verifyCursorPipelineValidateError(String value, String expectedError) {
        verifyCursorPipelineValidate(value, expectedError, false);
    }

    private static void verifyCursorPipelineValidateSuccess(String value) {
        verifyCursorPipelineValidate(value, null, true);
    }

    private static void verifyCursorPipelineValidate(String value, String expectedError, boolean success) {
        // Given:
        var config = mock(Configuration.class);
        var output = mock(Field.ValidationOutput.class);
        var errorMessage = ArgumentCaptor.forClass(String.class);
        var field = MongoDbConnectorConfig.CURSOR_PIPELINE;
        given(config.getString(field)).willReturn(value);

        doNothing().when(output).accept(eq(field), eq(value), errorMessage.capture());

        // When:
        field.validate(config, output);

        // Then:
        if (success) {
            assertThat(errorMessage.getAllValues())
                    .isEmpty();
        }
        else {
            assertThat(errorMessage.getAllValues())
                    .hasSize(1)
                    .element(0)
                    .isEqualTo(expectedError);
        }
    }

}
