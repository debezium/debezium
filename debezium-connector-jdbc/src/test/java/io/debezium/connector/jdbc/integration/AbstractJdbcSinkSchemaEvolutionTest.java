/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.junit.jupiter.SinkType;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.util.Strings;

/**
 * Common schema evolution tests.
 *
 * @author Chris Cranford
 */
public abstract class AbstractJdbcSinkSchemaEvolutionTest extends AbstractJdbcSinkTest {

    public AbstractJdbcSinkSchemaEvolutionTest(Sink sink) {
        super(sink);
    }

    @Override
    protected Map<String, String> getDefaultSinkConfig() {
        final Map<String, String> config = super.getDefaultSinkConfig();
        final String databaseSchemaName = getDatabaseSchemaName();
        if (!Strings.isNullOrBlank(databaseSchemaName)) {
            config.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, databaseSchemaName + ".${topic}");
        }
        return config;
    }

    protected String getDatabaseSchemaName() {
        return null;
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testCreateShouldFailIfSchemaEvolutionIsDisabled(SinkRecordFactory factory) {
        startSinkConnector(getDefaultSinkConfig());
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        try {
            consume(factory.createRecordNoKey(topicName));
            stopSinkConnector();
        }
        catch (Throwable t) {
            assertThat(t.getCause().getCause().getMessage()).startsWith("Could not find table: ");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testUpdateShouldFailOnUnknownTableIfSchemaEvolutionIsDisabled(SinkRecordFactory factory) {
        startSinkConnector(getDefaultSinkConfig());
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        try {
            consume(factory.updateRecord(topicName));
            stopSinkConnector();
        }
        catch (Throwable t) {
            assertThat(t.getCause().getCause().getMessage()).startsWith("Could not find table: ");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testDeleteShouldFailOnUnknownTableIfSchemaEvolutionIsDisabled(SinkRecordFactory factory) {
        startSinkConnector(getDefaultSinkConfig());
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        try {
            consume(factory.deleteRecord(topicName));
            stopSinkConnector();
        }
        catch (Throwable t) {
            assertThat(t.getCause().getCause().getMessage()).startsWith("Could not find table: ");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testTableCreatedOnCreateRecordWithDefaultInsertMode(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testTableCreatedOnUpdateRecordWithDefaultInsertMode(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord updateRecord = factory.updateRecord(topicName);
        consume(updateRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(updateRecord));
        tableAssert.hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "Jane Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testTableCreatedOnDeleteRecordWithDefaultInsertModeAndDeleteEnabled(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord deleteRecord = factory.deleteRecord(topicName);
        consume(deleteRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(deleteRecord));
        tableAssert.hasNumberOfRows(0).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testTableCreatedThenAlteredWithNewColumn(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName);
        consume(createRecord);

        final SinkRecord updateRecord = factory.updateBuilder()
                .name("prefix")
                .topic(topicName)
                .keySchema(factory.basicKeySchema())
                .recordSchema(SchemaBuilder.struct()
                        .field("id", Schema.INT8_SCHEMA)
                        .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                        .field("weight", Schema.OPTIONAL_INT32_SCHEMA))
                .sourceSchema(factory.basicSourceSchema())
                .key("id", (byte) 1)
                .before("id", (byte) 1)
                .before("name", "John Doe")
                .after("id", (byte) 1)
                .after("name", "John Doe")
                .after("age", 25)
                .after("weight", 150)
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
        consume(updateRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.hasNumberOfRows(2).hasNumberOfColumns(5);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$", null);
        getSink().assertColumnType(tableAssert, "age", ValueType.NUMBER, null, 25);
        getSink().assertColumnType(tableAssert, "weight", ValueType.NUMBER, null, 150);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testTableCreatedThenNotAlteredWithRemovedColumn(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName);
        consume(createRecord);

        final SinkRecord updateRecord = factory.updateBuilder()
                .name("prefix")
                .topic(topicName)
                .keySchema(factory.basicKeySchema())
                .recordSchema(SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA))
                .sourceSchema(factory.basicSourceSchema())
                .key("id", (byte) 1)
                .before("id", (byte) 1)
                .after("id", (byte) 1)
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
        consume(updateRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.hasNumberOfRows(2).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe", null);
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$", null);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testNonKeyColumnTypeResolutionFromKafkaSchemaType(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String text = "Hello World";
        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        // Create record, optionals provided.
        final SinkRecord createRecord = factory.createBuilder()
                .name("prefix")
                .topic(topicName)
                .keySchema(factory.basicKeySchema())
                .recordSchema(factory.allKafkaSchemaTypesSchema())
                .sourceSchema(factory.basicSourceSchema())
                .key("id", (byte) 1)
                .after("id", (byte) 1)
                .after("col_int8", (byte) 10)
                .after("col_int8_optional", (byte) 10)
                .after("col_int16", (short) 15)
                .after("col_int16_optional", (short) 15)
                .after("col_int32", 1024)
                .after("col_int32_optional", 1024)
                .after("col_int64", 1024L)
                .after("col_int64_optional", 1024L)
                .after("col_float32", 3.14f)
                .after("col_float32_optional", 3.14f)
                .after("col_float64", 3.14d)
                .after("col_float64_optional", 3.14d)
                .after("col_bool", true)
                .after("col_bool_optional", true)
                .after("col_string", text)
                .after("col_string_optional", text)
                .after("col_bytes", text.getBytes(StandardCharsets.UTF_8))
                .after("col_bytes_optional", text.getBytes(StandardCharsets.UTF_8))
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
        consume(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.hasNumberOfRows(1).hasNumberOfColumns(19);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "col_int8", ValueType.NUMBER, (byte) 10);
        getSink().assertColumnType(tableAssert, "col_int8_optional", ValueType.NUMBER, (byte) 10);
        getSink().assertColumnType(tableAssert, "col_int16", ValueType.NUMBER, (short) 15);
        getSink().assertColumnType(tableAssert, "col_int16_optional", ValueType.NUMBER, (short) 15);
        getSink().assertColumnType(tableAssert, "col_int32", ValueType.NUMBER, 1024);
        getSink().assertColumnType(tableAssert, "col_int32_optional", ValueType.NUMBER, 1024);
        getSink().assertColumnType(tableAssert, "col_int64", ValueType.NUMBER, 1024L);
        getSink().assertColumnType(tableAssert, "col_int64_optional", ValueType.NUMBER, 1024L);
        getSink().assertColumnType(tableAssert, "col_float32", ValueType.NUMBER, 3.14f);
        getSink().assertColumnType(tableAssert, "col_float32_optional", ValueType.NUMBER, 3.14f);
        getSink().assertColumnType(tableAssert, "col_float64", ValueType.NUMBER, 3.14d);
        getSink().assertColumnType(tableAssert, "col_float64_optional", ValueType.NUMBER, 3.14d);
        getSink().assertColumnType(tableAssert, "col_string", ValueType.TEXT, text);
        getSink().assertColumnType(tableAssert, "col_string_optional", ValueType.TEXT, text);
        getSink().assertColumnType(tableAssert, "col_bytes", ValueType.BYTES, text.getBytes(StandardCharsets.UTF_8));
        getSink().assertColumnType(tableAssert, "col_bytes_optional", ValueType.BYTES, text.getBytes(StandardCharsets.UTF_8));
        if (getSink().getType().is(SinkType.ORACLE)) {
            getSink().assertColumnType(tableAssert, "col_bool", ValueType.NUMBER, 1);
            getSink().assertColumnType(tableAssert, "col_bool_optional", ValueType.NUMBER, 1);
        }
        else {
            getSink().assertColumnType(tableAssert, "col_bool", ValueType.BOOLEAN, true);
            getSink().assertColumnType(tableAssert, "col_bool_optional", ValueType.BOOLEAN, true);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testNonKeyColumnTypeResolutionFromKafkaSchemaTypeWithOptionalsWithDefaultValues(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String text = "Hello World";
        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        // Create record, optionals provided.
        final SinkRecord createRecord = factory.createBuilder()
                .name("prefix")
                .topic(topicName)
                .keySchema(factory.basicKeySchema())
                .recordSchema(factory.allKafkaSchemaTypesSchemaWithOptionalDefaultValues())
                .sourceSchema(factory.basicSourceSchema())
                .key("id", (byte) 1)
                .after("id", (byte) 1)
                .after("col_int8", (byte) 10)
                .after("col_int16", (short) 15)
                .after("col_int32", 1024)
                .after("col_int64", 1024L)
                .after("col_float32", 3.14f)
                .after("col_float64", 3.14d)
                .after("col_bool", true)
                .after("col_string", text)
                .after("col_bytes", text.getBytes(StandardCharsets.UTF_8))
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
        consume(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.hasNumberOfRows(1).hasNumberOfColumns(19);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "col_int8", ValueType.NUMBER, (byte) 10);
        getSink().assertColumnHasNullValue(tableAssert, "col_int8_optional");
        getSink().assertColumnType(tableAssert, "col_int16", ValueType.NUMBER, (short) 15);
        getSink().assertColumnHasNullValue(tableAssert, "col_int16_optional");
        getSink().assertColumnType(tableAssert, "col_int32", ValueType.NUMBER, 1024);
        getSink().assertColumnHasNullValue(tableAssert, "col_int32_optional");
        getSink().assertColumnType(tableAssert, "col_int64", ValueType.NUMBER, 1024L);
        getSink().assertColumnHasNullValue(tableAssert, "col_int64_optional");
        getSink().assertColumnType(tableAssert, "col_float32", ValueType.NUMBER, 3.14f);
        getSink().assertColumnHasNullValue(tableAssert, "col_float32_optional");
        getSink().assertColumnType(tableAssert, "col_float64", ValueType.NUMBER, 3.14d);
        getSink().assertColumnHasNullValue(tableAssert, "col_float64_optional");
        getSink().assertColumnType(tableAssert, "col_string", ValueType.TEXT, text);
        getSink().assertColumnHasNullValue(tableAssert, "col_string_optional");
        getSink().assertColumnType(tableAssert, "col_bytes", ValueType.BYTES, text.getBytes(StandardCharsets.UTF_8));
        getSink().assertColumnHasNullValue(tableAssert, "col_bytes_optional");
        if (getSink().getType().is(SinkType.ORACLE)) {
            getSink().assertColumnType(tableAssert, "col_bool", ValueType.NUMBER, 1);
            getSink().assertColumnHasNullValue(tableAssert, "col_bool_optional");
        }
        else {
            getSink().assertColumnType(tableAssert, "col_bool", ValueType.BOOLEAN, true);
            getSink().assertColumnHasNullValue(tableAssert, "col_bool_optional");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void shouldCreateTableWithDefaultValues(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final Schema recordSchemaCreate = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("col_int8", SchemaBuilder.int8().defaultValue((byte) 2).build())
                .field("col_int8_optional", SchemaBuilder.int8().optional().defaultValue((byte) 2).build())
                .field("col_int16", SchemaBuilder.int16().defaultValue((short) 2).build())
                .field("col_int16_optional", SchemaBuilder.int16().optional().defaultValue((short) 2).build())
                .field("col_int32", SchemaBuilder.int32().defaultValue(2048).build())
                .field("col_int32_optional", SchemaBuilder.int32().optional().defaultValue(2048).build())
                .field("col_int64", SchemaBuilder.int64().defaultValue(2048L).build())
                .field("col_int64_optional", SchemaBuilder.int64().optional().defaultValue(2048L).build())
                .field("col_float32", SchemaBuilder.float32().defaultValue(2.34f).build())
                .field("col_float32_optional", SchemaBuilder.float32().optional().defaultValue(2.34f).build())
                .field("col_float64", SchemaBuilder.float64().defaultValue(1.23d).build())
                .field("col_float64_optional", SchemaBuilder.float64().optional().defaultValue(1.23d).build())
                .field("col_bool", SchemaBuilder.bool().defaultValue(true).build())
                .field("col_bool_optional", SchemaBuilder.bool().optional().defaultValue(true).build())
                .field("col_string", SchemaBuilder.string().defaultValue("test").build())
                .field("col_string_optional", SchemaBuilder.string().optional().defaultValue("test").build())
                .field("col_bytes", SchemaBuilder.bytes().defaultValue("test".getBytes()).build())
                .field("col_bytes_optional", SchemaBuilder.bytes().optional().defaultValue("test".getBytes()).build())
                .build();

        final String text = "Hello World";
        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        // Create record, optionals provided.
        final SinkRecord createRecord = factory.createBuilder()
                .name("prefix")
                .topic(topicName)
                .keySchema(factory.basicKeySchema())
                .recordSchema(recordSchemaCreate)
                .sourceSchema(factory.basicSourceSchema())
                .key("id", (byte) 1)
                .after("id", (byte) 1)
                .after("col_int8_optional", (byte) 2)
                .after("col_int16_optional", (short) 2)
                .after("col_int32_optional", 2048)
                .after("col_int64_optional", 2048L)
                .after("col_float32_optional", 2.34f)
                .after("col_float64_optional", 1.23d)
                .after("col_bool_optional", true)
                .after("col_string_optional", text)
                .after("col_bytes_optional", text.getBytes(StandardCharsets.UTF_8))
                .source("ts_ms", (int) Instant.now().getEpochSecond())
                .build();
        consume(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.hasNumberOfRows(1).hasNumberOfColumns(19);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "col_int8", ValueType.NUMBER, (byte) 2);
        getSink().assertColumnType(tableAssert, "col_int8_optional", ValueType.NUMBER, (byte) 2);
        getSink().assertColumnType(tableAssert, "col_int16", ValueType.NUMBER, (short) 2);
        getSink().assertColumnType(tableAssert, "col_int16_optional", ValueType.NUMBER, (short) 2);
        getSink().assertColumnType(tableAssert, "col_int32", ValueType.NUMBER, 2048);
        getSink().assertColumnType(tableAssert, "col_int32_optional", ValueType.NUMBER, 2048);
        getSink().assertColumnType(tableAssert, "col_int64", ValueType.NUMBER, 2048L);
        getSink().assertColumnType(tableAssert, "col_int64_optional", ValueType.NUMBER, 2048L);
        getSink().assertColumnType(tableAssert, "col_float32", ValueType.NUMBER, 2.34f);
        getSink().assertColumnType(tableAssert, "col_float32_optional", ValueType.NUMBER, 2.34f);
        getSink().assertColumnType(tableAssert, "col_float64", ValueType.NUMBER, 1.23d);
        getSink().assertColumnType(tableAssert, "col_float64_optional", ValueType.NUMBER, 1.23d);
        getSink().assertColumnType(tableAssert, "col_string", ValueType.TEXT, "test");
        getSink().assertColumnType(tableAssert, "col_string_optional", ValueType.TEXT, text);
        getSink().assertColumnType(tableAssert, "col_bytes", ValueType.BYTES, "test".getBytes(StandardCharsets.UTF_8));
        getSink().assertColumnType(tableAssert, "col_bytes_optional", ValueType.BYTES, text.getBytes(StandardCharsets.UTF_8));
        if (getSink().getType().is(SinkType.ORACLE)) {
            getSink().assertColumnType(tableAssert, "col_bool", ValueType.NUMBER, 1);
            getSink().assertColumnType(tableAssert, "col_bool_optional", ValueType.NUMBER, 1);
        }
        else {
            getSink().assertColumnType(tableAssert, "col_bool", ValueType.BOOLEAN, true);
            getSink().assertColumnType(tableAssert, "col_bool_optional", ValueType.BOOLEAN, true);
        }
    }

}
