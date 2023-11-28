/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.debezium.doc.FixFor;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.PrimaryKeyMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;

/**
 * Common insert mode tests.
 *
 * @author Chris Cranford
 */
public abstract class AbstractJdbcSinkInsertModeTest extends AbstractJdbcSinkTest {

    public AbstractJdbcSinkInsertModeTest(Sink sink) {
        super(sink);
    }

    // InsertMode: INSERT, UPSERT, UPDATE
    //
    // Need to test all modes with and without primary keys.
    // UPSERT will fail if no primary keys are defined.

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeInsertWithNoPrimaryKey(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);
        consume(factory.createRecordNoKey(topicName));

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe", "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$", "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeInsertWithPrimaryKeyModeKafka(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.KAFKA.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);
        consume(factory.createRecord(topicName));

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(6);

        getSink().assertColumnType(tableAssert, "__connect_topic", ValueType.TEXT, topicName, topicName);
        getSink().assertColumnType(tableAssert, "__connect_partition", ValueType.NUMBER, 0, 0);
        getSink().assertColumnType(tableAssert, "__connect_offset", ValueType.NUMBER, 0, 1);
        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe", "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$", "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeInsertWithPrimaryKeyModeRecordKey(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName, (byte) 1);
        consume(createRecord);
        consume(factory.createRecord(topicName, (byte) 2));

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 2);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe", "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$", "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeInsertWithPrimaryKeyModeRecordValue(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName, (byte) 1);
        consume(createRecord);
        consume(factory.createRecord(topicName, (byte) 2));

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 2);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe", "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$", "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeUpsertWithNoPrimaryKey(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        try {
            consume(factory.createRecordNoKey(topicName));
            stopSinkConnector();
        }
        catch (Exception e) {
            assertThat(e.getCause().getCause().getMessage()).matches(
                    "Cannot write to table [a-zA-Z0-9_]* with no key fields defined\\.");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeUpsertWithPrimaryKeyModeKafka(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.KAFKA.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName, (byte) 1);
        consume(createRecord);
        consume(factory.createRecord(topicName, (byte) 1));

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(6);

        getSink().assertColumnType(tableAssert, "__connect_topic", ValueType.TEXT, topicName);
        getSink().assertColumnType(tableAssert, "__connect_partition", ValueType.NUMBER, 0);
        getSink().assertColumnType(tableAssert, "__connect_offset", ValueType.NUMBER, 1L);
        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeUpsertWithPrimaryKeyModeRecordKey(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName, (byte) 1);
        consume(createRecord);
        consume(factory.createRecord(topicName, (byte) 1));

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeUpsertWithPrimaryKeyModeRecordValue(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName, (byte) 1);
        consume(createRecord);
        consume(factory.createRecord(topicName, (byte) 1));

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeUpdateWithNoPrimaryKey(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPDATE.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);

        // No changes detected because there is no existing record.
        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(0).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeUpdateWithPrimaryKeyModeKafka(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.KAFKA.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPDATE.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName);
        consume(createRecord);

        // No changes detected because there is no existing record.
        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(0).hasNumberOfColumns(6);

        getSink().assertColumnType(tableAssert, "__connect_topic", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "__connect_partition", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "__connect_offset", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeUpdateWithPrimaryKeyModeRecordKey(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPDATE.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName);
        consume(createRecord);

        // No changes detected because there is no existing record.
        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(0).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeUpdateWithPrimaryKeyModeRecordValue(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPDATE.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName);
        consume(createRecord);

        // No changes detected because there is no existing record.
        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(0).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT);
    }

    @FixFor("DBZ-7191")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordDefaultValueUsedOnlyWithRequiredFieldWithNullValue(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecordWithSchemaValue(topicName,
                (byte) 1,
                List.of( "optional_with_default_null_value"),
                List.of(SchemaBuilder.string().defaultValue("default").optional().build()),
                Arrays.asList(new Object[]{null}));

        consume(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnHasNullValue(tableAssert, "optional_with_default_null_value");
    }
}
