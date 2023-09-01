/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.PrimaryKeyMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.junit.Assume;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.Map;

/**
 * Common delete enabled tests.
 *
 * @author Chris Cranford
 */
public abstract class AbstractJdbcSinkDeleteEnabledTest extends AbstractJdbcSinkTest {

    public AbstractJdbcSinkDeleteEnabledTest(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldNotDeleteRowWhenDeletesDisabled(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "false");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName);
        consume(createRecord);
        consume(factory.deleteRecord(topicName));

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldDeleteRowWhenDeletesEnabled(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecord(topicName);
        consume(createRecord);
        consume(factory.deleteRecord(topicName));

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(0).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldDeleteRowWhenDeletesEnabledUsingSubsetOfRecordKeyFields(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id2");
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createRecord = factory.createRecordMultipleKeyColumns(topicName);
        consume(createRecord);
        consume(factory.deleteRecordMultipleKeyColumns(topicName));

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(0).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id1", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "id2", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldHandleRowDeletionWhenRowDoesNotExist(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord deleteRecord = factory.deleteRecord(topicName);
        consume(deleteRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(deleteRecord));
        tableAssert.exists().hasNumberOfRows(0).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldHandleRowDeletionWhenRowDoesNotExistUsingSubsetOfRecordKeyFields(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id2");
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord deleteRecord = factory.deleteRecordMultipleKeyColumns(topicName);
        consume(deleteRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(deleteRecord));
        tableAssert.exists().hasNumberOfRows(0).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id1", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "id2", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6862")
    public void testShouldSkipTombstoneRecord(SinkRecordFactory factory) {

        Assumptions.assumeFalse(factory.isFlattened());

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord deleteRecord = factory.deleteRecord(topicName);
        //Switching the normal order for test purpose.
        // If the delete record is not processed mean that the tombstone generated an error
        consume(factory.tombstoneRecord(topicName));
        consume(deleteRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(deleteRecord));
        tableAssert.exists().hasNumberOfRows(0).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT);
    }

}
