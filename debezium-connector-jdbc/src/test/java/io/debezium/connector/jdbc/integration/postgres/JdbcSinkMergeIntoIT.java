/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.lettercase.CaseComparisons;
import org.assertj.db.type.lettercase.CaseConversions;
import org.assertj.db.type.lettercase.LetterCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkInsertModeTest;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.FlatSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Insert Mode tests for PostgreSQL.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkMergeIntoIT extends AbstractJdbcSinkInsertModeTest {

    public static final LetterCase LOWER_CASE_STRICT = LetterCase.getLetterCase(CaseConversions.LOWER, CaseComparisons.STRICT);
    public static final LetterCase UPPER_CASE_STRICT = LetterCase.getLetterCase(CaseConversions.UPPER, CaseComparisons.STRICT);

    public JdbcSinkMergeIntoIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeUpsertDoesNotHandlePkChange(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "public", tableName);

        final KafkaDebeziumSinkRecord createSimpleRecord1 = factory.createRecord(topicName, (byte) 1, String::toUpperCase);
        final KafkaDebeziumSinkRecord createSimpleRecord2 = factory.updateRecord(topicName, (byte) 1, (byte) 2, "Jane Doe");
        consume(createSimpleRecord1);
        consume(createSimpleRecord2);

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(createSimpleRecord1), null, null);
        tableAssert.exists().hasNumberOfRows(2);

        tableAssert.row(0).value("id").isEqualTo(1);
        tableAssert.row(1).value("id").isEqualTo(2);
        tableAssert.row(0).value("name").isEqualTo("John Doe");
        tableAssert.row(1).value("name").isEqualTo("Jane Doe");
    }

    @Test
    public void testInsertModeMergeIntoHandlePkChange() throws Exception {
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.MERGE_INTO.getValue());

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "public", tableName);

        final KafkaDebeziumSinkRecord createSimpleRecord1 = factory.createRecord(topicName, (byte) 1, String::toUpperCase);
        final KafkaDebeziumSinkRecord createSimpleRecord2 = factory.updateRecord(topicName, (byte) 1, (byte) 2, "Jane Doe");
        consume(createSimpleRecord1);
        consume(createSimpleRecord2);

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(createSimpleRecord1), null, null);
        tableAssert.exists().hasNumberOfRows(1);

        tableAssert.row(0).value("id").isEqualTo(2);
        tableAssert.row(0).value("name").isEqualTo("Jane Doe");
    }

    @Test
    public void testInsertModeMergeIntoCrashOnFlatRecord() throws Exception {
        final SinkRecordFactory factory = new FlatSinkRecordFactory();
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.MERGE_INTO.getValue());

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "public", tableName);

        final KafkaDebeziumSinkRecord createSimpleRecord1 = factory.createRecord(topicName, (byte) 1, String::toUpperCase);
        final KafkaDebeziumSinkRecord createSimpleRecord2 = factory.updateRecord(topicName, (byte) 1, (byte) 2, "Jane Doe");
        consume(createSimpleRecord1);

        Assertions.assertThrows(ConnectException.class, () -> {
            consume(createSimpleRecord2);
        });
    }

    @Test
    public void testInsertModeMergeIntoHandleNewRecordWithPkChange() throws Exception {
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.MERGE_INTO.getValue());

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "public", tableName);

        final KafkaDebeziumSinkRecord createSimpleRecord1 = factory.updateRecord(topicName, (byte) 1, (byte) 2, "Jane Doe");
        consume(createSimpleRecord1);

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(createSimpleRecord1), null, null);
        tableAssert.exists().hasNumberOfRows(1);

        tableAssert.row(0).value("id").isEqualTo(2);
        tableAssert.row(0).value("name").isEqualTo("Jane Doe");
    }

    @Test
    public void testInsertModeMergeIntoPkMoveAndReplay() throws Exception {
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.MERGE_INTO.getValue());

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "public", tableName);

        final KafkaDebeziumSinkRecord createSimpleRecord1 = factory.createRecord(topicName, (byte) 1, String::toUpperCase);
        final KafkaDebeziumSinkRecord createSimpleRecord2 = factory.updateRecord(topicName, (byte) 1, (byte) 2, "Jane Doe");
        consume(createSimpleRecord1);
        consume(createSimpleRecord2);

        TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(createSimpleRecord1), null, null);
        tableAssert.exists().hasNumberOfRows(1);

        tableAssert.row(0).value("id").isEqualTo(2);
        tableAssert.row(0).value("name").isEqualTo("Jane Doe");

        consume(createSimpleRecord1);
        consume(createSimpleRecord2);

        tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(createSimpleRecord1), null, null);
        tableAssert.exists().hasNumberOfRows(1);
        tableAssert.row(0).value("id").isEqualTo(2);
        tableAssert.row(0).value("name").isEqualTo("Jane Doe");
    }

}
