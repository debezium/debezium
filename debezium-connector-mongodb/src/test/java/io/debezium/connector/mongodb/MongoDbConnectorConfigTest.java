/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.connection.MongoDbConnectionContext;
import io.debezium.data.Envelope;

public class MongoDbConnectorConfigTest {

    @Test
    void shouldExposeCaptureStartFields() {
        final var configDef = new MongoDbConnector().config();
        for (var field : new Field[]{ MongoDbConnectorConfig.CAPTURE_START_OP_TIME, MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP }) {
            assertThat(MongoDbConnectorConfig.ALL_FIELDS.fieldWithName(field.name())).isNotNull();
            assertThat(configDef.configKeys()).containsKey(field.name());
        }
    }

    @Test
    void shouldLeaveStartTimeUnspecifiedByDefault() {
        final var config = TestHelper.getConfiguration();
        assertThat(new MongoDbConnectorConfig(config).startAtOperationTime()).isEmpty();
        assertThat(config.validate(MongoDbConnectorConfig.ALL_FIELDS).get(MongoDbConnectorConfig.CAPTURE_START_OP_TIME.name()).errorMessages()).isEmpty();
        assertThat(config.validate(MongoDbConnectorConfig.ALL_FIELDS).get(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP.name()).errorMessages()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(longs = { 0L, 30L, 7684060705770700807L, Long.MIN_VALUE, Long.MAX_VALUE })
    void shouldPreservePackedOperationTime(long value) {
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_START_OP_TIME, value)
                .build();
        assertThat(new MongoDbConnectorConfig(config).startAtOperationTime()).contains(new BsonTimestamp(value));
        assertThat(config.validate(MongoDbConnectorConfig.ALL_FIELDS).get(MongoDbConnectorConfig.CAPTURE_START_OP_TIME.name()).errorMessages()).isEmpty();
        assertThat(config.validate(MongoDbConnectorConfig.ALL_FIELDS).get(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP.name()).errorMessages()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "1789084800", "2026-09-11T09:00:00+09:00", "{\"$timestamp\":{\"t\":1789084800,\"i\":0}}" })
    void shouldUseReadableStartTimestamp(String value) {
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP, value)
                .build();
        assertThat(config.validate(MongoDbConnectorConfig.ALL_FIELDS).get(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP.name()).errorMessages()).isEmpty();
        assertThat(config.validate(MongoDbConnectorConfig.ALL_FIELDS).get(MongoDbConnectorConfig.CAPTURE_START_OP_TIME.name()).errorMessages()).isEmpty();
        assertThat(new MongoDbConnectorConfig(config).startAtOperationTime()).contains(new BsonTimestamp(1789084800, 0));
    }

    @Test
    void shouldAllowDisabledLegacyStartTimeWithReadableTimestamp() {
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_START_OP_TIME, -1L)
                .with(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP, "30")
                .build();
        assertThat(new MongoDbConnectorConfig(config).startAtOperationTime()).contains(new BsonTimestamp(30, 0));
        assertThat(config.validate(MongoDbConnectorConfig.ALL_FIELDS).get(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP.name()).errorMessages()).isEmpty();
        assertThat(config.validate(MongoDbConnectorConfig.ALL_FIELDS).get(MongoDbConnectorConfig.CAPTURE_START_OP_TIME.name()).errorMessages()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "30", "", "invalid" })
    void shouldRejectConflictingStartTimes(String value) {
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_START_OP_TIME, new BsonTimestamp(30, 0).getValue())
                .with(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP, value)
                .build();
        final var connector = new ConnectionValidationConnector();
        final var validation = connector.validate(config.asMap());
        assertThat(connector.connectionConfig).isNull();
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CONNECTION_STRING)).isEmpty();
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP))
                .containsExactly("The 'capture.start.timestamp' value is invalid: Cannot be configured together with 'capture.start.op.time'");
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_OP_TIME))
                .containsExactly("The 'capture.start.op.time' value is invalid: Cannot be configured together with 'capture.start.timestamp'");
        assertThatThrownBy(() -> new MongoDbConnectorConfig(config))
                .isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value " + value + " for configuration capture.start.timestamp: Cannot be configured together with 'capture.start.op.time'");
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "invalid", "30.5", "4294967296", "2026-09-11T00:00:00.001Z", "{\"$timestamp\":{\"t\":30,\"i\":-1}}" })
    void shouldReportInvalidStartTimestampAsConfigurationError(String value) {
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP, value)
                .build();
        final var connector = new ConnectionValidationConnector();
        final var validation = connector.validate(config.asMap());
        assertThat(connector.connectionConfig).isNull();
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CONNECTION_STRING)).isEmpty();
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_OP_TIME)).isEmpty();
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP))
                .singleElement().asString()
                .startsWith("The 'capture.start.timestamp' value is invalid: ")
                .doesNotContain("Invalid value", "for configuration");
        assertThatThrownBy(() -> new MongoDbConnectorConfig(config))
                .isInstanceOf(ConfigException.class)
                .hasMessageStartingWith("Invalid value " + value + " for configuration capture.start.timestamp: ");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "30")
    void shouldValidateLegacyStartTimeType(String value) {
        final var builder = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_START_OP_TIME, "invalid");
        if (value != null) {
            builder.with(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP, value);
        }
        final var config = builder.build();
        final var connector = new ConnectionValidationConnector();
        final var validation = connector.validate(config.asMap());
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_OP_TIME))
                .containsExactly("The 'capture.start.op.time' value is invalid: A long value is expected");
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP)).isEmpty();
        assertThat(connector.connectionConfig).isNotNull();
        assertThat(connector.connectionConfig.startAtOperationTime())
                .isEqualTo(value == null ? Optional.empty() : Optional.of(new BsonTimestamp(30, 0)));
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CONNECTION_STRING))
                .containsExactly(ConnectionValidationConnector.CONNECTION_ERROR);
    }

    @Test
    void shouldReportMalformedStartTimesOnTheirOwnFields() {
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_START_OP_TIME, "invalid")
                .with(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP, "invalid")
                .build();
        final var connector = new ConnectionValidationConnector();
        final var validation = connector.validate(config.asMap());
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_OP_TIME))
                .containsExactly("The 'capture.start.op.time' value is invalid: A long value is expected");
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP))
                .singleElement().asString().startsWith("The 'capture.start.timestamp' value is invalid: Expected integer Unix seconds");
        assertThat(connector.connectionConfig).isNull();
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CONNECTION_STRING)).isEmpty();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "30")
    void shouldValidateConnectionWithValidStartTimestamp(String value) {
        final var builder = TestHelper.getConfiguration().edit();
        if (value != null) {
            builder.with(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP, value);
        }
        final var config = builder.build();
        final var connector = new ConnectionValidationConnector();
        final var validation = connector.validate(config.asMap());
        assertThat(connector.connectionConfig).isNotNull();
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP)).isEmpty();
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CONNECTION_STRING))
                .containsExactly(ConnectionValidationConnector.CONNECTION_ERROR);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "30")
    void shouldValidateConnectionWithUnrelatedFieldError(String value) {
        final var builder = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, "invalid");
        if (value != null) {
            builder.with(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP, value);
        }
        final var config = builder.build();
        final var connector = new ConnectionValidationConnector();
        final var validation = connector.validate(config.asMap());
        assertThat(validationErrors(validation, MongoDbConnectorConfig.SNAPSHOT_MODE)).hasSize(1);
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP)).isEmpty();
        assertThat(connector.connectionConfig).isNotNull();
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CONNECTION_STRING))
                .containsExactly(ConnectionValidationConnector.CONNECTION_ERROR);
    }

    @Test
    void shouldSkipConnectionValidationWithInvalidConnectionString() {
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CONNECTION_STRING, "invalid")
                .with(MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP, "30")
                .build();
        final var connector = new ConnectionValidationConnector();
        final var validation = connector.validate(config.asMap());
        assertThat(connector.connectionConfig).isNull();
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CONNECTION_STRING))
                .isNotEmpty().doesNotContain(ConnectionValidationConnector.CONNECTION_ERROR);
        assertThat(validationErrors(validation, MongoDbConnectorConfig.CAPTURE_START_TIMESTAMP)).isEmpty();
    }

    private static List<String> validationErrors(Config validation, Field field) {
        return validation.configValues().stream()
                .filter(value -> value.name().equals(field.name()))
                .findFirst().orElseThrow().errorMessages();
    }

    private static class ConnectionValidationConnector extends MongoDbConnector {
        private static final String CONNECTION_ERROR = "Unable to connect: test connection failure";

        private MongoDbConnectorConfig connectionConfig;

        @Override
        public void validateConnection(Configuration config, ConfigValue connectionStringValidation) {
            // Exercise configuration construction without opening a MongoDB connection.
            connectionConfig = new MongoDbConnectionContext(config).getConnectorConfig();
            connectionStringValidation.addErrorMessage(CONNECTION_ERROR);
        }
    }

    @Test
    void parseSignallingMessage() {
        Schema schema = new SchemaBuilder(Schema.Type.STRUCT).field("after", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(schema);
        struct.put("after", "{\"_id\":\"test-1\"," +
                "\"type\":\"execute-snapshot\"," +
                "\"data\":{\"data-collections\":[\"database.collection\"],\"type\":\"incremental\"}}");
        MongoDbConnectorConfig mongoDbConnectorConfig = new MongoDbConnectorConfig(TestHelper.getConfiguration());

        Optional<String[]> resultOpt = mongoDbConnectorConfig.parseSignallingMessage(struct, Envelope.FieldName.AFTER);

        Assertions.assertTrue(resultOpt.isPresent());
        String[] result = resultOpt.get();
        Assertions.assertEquals(3, result.length);
        Assertions.assertEquals("test-1", result[0]);
        Assertions.assertEquals("execute-snapshot", result[1]);
        Assertions.assertEquals("{\"data-collections\": [\"database.collection\"], \"type\": \"incremental\"}", result[2]);
    }

    @Test
    void parseCursorPipeline() {
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

    @Test
    void captureScopeAndTargetAreValidatedFields() {
        // capture.target validation only runs when the fields are part of ALL_FIELDS
        Assertions.assertNotNull(MongoDbConnectorConfig.ALL_FIELDS.fieldWithName(MongoDbConnectorConfig.CAPTURE_SCOPE.name()));
        Assertions.assertNotNull(MongoDbConnectorConfig.ALL_FIELDS.fieldWithName(MongoDbConnectorConfig.CAPTURE_TARGET.name()));
    }

    @Test
    void validateCaptureTarget() {
        // deployment scope does not use capture.target
        verifyCaptureTargetValidateSuccess("deployment", null);
        verifyCaptureTargetValidateSuccess("deployment", "inventory");
        verifyCaptureTargetValidateSuccess(null, null);

        // database scope requires a database name
        verifyCaptureTargetValidateSuccess("database", "inventory");
        verifyCaptureTargetValidateError("database", null,
                "The 'capture.target' property must be set to a database name when 'capture.scope' is 'database'");

        // collection scope requires <databaseName>.<collectionName>
        verifyCaptureTargetValidateSuccess("collection", "inventory.orders");
        for (String invalid : new String[]{ null, "inventory", "inventory.", ".orders", "inventory.orders.archive" }) {
            verifyCaptureTargetValidateError("collection", invalid,
                    "The 'capture.target' property must be set to '<databaseName>.<collectionName>' when 'capture.scope' is 'collection'");
        }
    }

    private static void verifyCaptureTargetValidateError(String scope, String value, String expectedError) {
        verifyCaptureTargetValidate(scope, value, expectedError, false);
    }

    private static void verifyCaptureTargetValidateSuccess(String scope, String value) {
        verifyCaptureTargetValidate(scope, value, null, true);
    }

    private static void verifyCaptureTargetValidate(String scope, String value, String expectedError, boolean success) {
        // Given:
        var config = mock(Configuration.class);
        var output = mock(Field.ValidationOutput.class);
        var errorMessage = ArgumentCaptor.forClass(String.class);
        var field = MongoDbConnectorConfig.CAPTURE_TARGET;
        given(config.getString(field)).willReturn(value);
        given(config.getString(MongoDbConnectorConfig.CAPTURE_SCOPE)).willReturn(scope);

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
