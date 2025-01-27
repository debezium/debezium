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
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Field filter tests for PostgreSQL.
 *
 * @author Anisha Mohanty
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkFieldFilterIT extends AbstractJdbcSinkTest {

    public JdbcSinkFieldFilterIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6636")
    public void testFieldIncludeListWithInsertMode(SinkRecordFactory factory) throws Exception {
        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.INSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.FIELD_INCLUDE_LIST, topicName + ":name," + topicName + ":id");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(createRecord));
        tableAssert.exists().column("id").value().isEqualTo(1);
        tableAssert.exists().column("name").value().isEqualTo("John Doe");
        tableAssert.exists().hasNumberOfColumns(2);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6636")
    public void testFieldExcludeListWithInsertMode(SinkRecordFactory factory) throws Exception {
        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.INSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.FIELD_EXCLUDE_LIST, topicName + ":name");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, 1);
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6636")
    public void testFieldIncludeListWithUpsertMode(SinkRecordFactory factory) throws Exception {
        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.FIELD_INCLUDE_LIST, topicName + ":name," + topicName + ":id");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final KafkaDebeziumSinkRecord createRecord = factory.createRecord(topicName, (byte) 1);
        consume(createRecord);
        consume(factory.createRecord(topicName, (byte) 1));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(createRecord));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");

        final KafkaDebeziumSinkRecord updateRecord = factory.updateRecord(topicName);
        consume(updateRecord);

        final TableAssert tableAssertForUpdate = TestHelper.assertTable(assertDbConnection(), destinationTableName(updateRecord));
        tableAssertForUpdate.exists().hasNumberOfRows(1).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssertForUpdate, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssertForUpdate, "name", ValueType.TEXT, "Jane Doe");
    }
}
