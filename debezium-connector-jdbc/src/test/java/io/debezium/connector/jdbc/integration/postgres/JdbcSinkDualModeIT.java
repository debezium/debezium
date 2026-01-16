/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import java.util.Map;

import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.PostgresInsertModeArgumentsProvider;
import io.debezium.connector.jdbc.junit.jupiter.PostgresInsertModeArgumentsProvider.PostgresInsertMode;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Example test class demonstrating how to use {@link PostgresInsertModeArgumentsProvider}
 * to run tests with both standard INSERT and UNNEST optimization modes.
 *
 * This approach provides comprehensive test coverage:
 * - Each test method runs 4 times (2 record factories x 2 insert modes)
 * - DebeziumSinkRecordFactory + Standard INSERT
 * - DebeziumSinkRecordFactory + UNNEST
 * - FlatSinkRecordFactory + Standard INSERT
 * - FlatSinkRecordFactory + UNNEST
 *
 * @author Gaurav Miglani
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkDualModeIT extends AbstractJdbcSinkTest {

    public JdbcSinkDualModeIT(Sink sink) {
        super(sink);
    }

    /**
     * Example test that runs with both INSERT modes automatically.
     * The test logic is the same; only the underlying INSERT strategy changes.
     */
    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-1525")
    public void testInsertWithBothModes(SinkRecordFactory factory, PostgresInsertMode insertMode) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());

        // Configure UNNEST based on the parameter
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecord(topicName, (byte) 1);
        consume(createRecord);
        consume(factory.createRecord(topicName, (byte) 2));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 2);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe", "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$", "John Doe$");
    }

    /**
     * Example test for UPSERT mode with both INSERT strategies.
     */
    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-1525")
    public void testUpsertWithBothModes(SinkRecordFactory factory, PostgresInsertMode insertMode) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecord(topicName, (byte) 1);
        consume(createRecord);
        consume(factory.createRecord(topicName, (byte) 1)); // Same ID - should upsert

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");
    }

    /**
     * Example test for batch operations with both modes.
     * This is where UNNEST optimization shows significant performance gains.
     */
    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-1525")
    public void testBatchInsertWithBothModes(SinkRecordFactory factory, PostgresInsertMode insertMode) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.BATCH_SIZE, "100");
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        // Insert 10 records in batch
        for (byte i = 1; i <= 10; i++) {
            consume(factory.createRecord(topicName, i));
        }

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(),
                destinationTableName(factory.createRecord(topicName, (byte) 1)));
        tableAssert.exists().hasNumberOfRows(10).hasNumberOfColumns(3);
    }
}
