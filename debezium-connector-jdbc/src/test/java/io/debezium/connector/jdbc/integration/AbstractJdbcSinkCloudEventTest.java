/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.fest.assertions.Index;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.converters.spi.SerializerType;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Common converted CloudEvent saving tests.
 *
 * @author Roman Kudryashov
 * @author rk3rn3r
 */
public abstract class AbstractJdbcSinkCloudEventTest extends AbstractJdbcSinkTest {

    public AbstractJdbcSinkCloudEventTest(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testCloudEventRecordFromJson(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord cloudEventRecord = factory.cloudEventRecord(topicName, SerializerType.withName("json"));
        final KafkaDebeziumSinkRecord convertedRecord = new KafkaDebeziumSinkRecord(cloudEventRecord.getOriginalKafkaRecord(),
                new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
        consume(convertedRecord);

        final String destinationTableName = destinationTableName(convertedRecord);

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName);
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "Jane Doe");
        getSink().assertColumnType(tableAssert, "nick_name_", ValueType.TEXT, "John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName, "id");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testCloudEventRecordFromAvro(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord cloudEventRecord = factory.cloudEventRecord(topicName, SerializerType.withName("avro"));
        final KafkaDebeziumSinkRecord convertedRecord = new KafkaDebeziumSinkRecord(cloudEventRecord.getOriginalKafkaRecord(),
                new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
        consume(convertedRecord);

        final String destinationTableName = destinationTableName(convertedRecord);

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName);
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "Jane Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName, "id");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testCloudEventRecordFromAvroWithCloudEventsSchemaCustomName(SinkRecordFactory factory) {
        final String cloudEventsSchemaName = "TestCESchemaCustomName";
        final String cloudEventsSchemaNamePattern = ".*" + cloudEventsSchemaName + ".*";
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.CLOUDEVENTS_SCHEMA_NAME_PATTERN, cloudEventsSchemaNamePattern);
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord cloudEventRecord = factory.cloudEventRecord(topicName, SerializerType.withName("avro"), cloudEventsSchemaName);
        final KafkaDebeziumSinkRecord convertedRecord = new KafkaDebeziumSinkRecord(cloudEventRecord.getOriginalKafkaRecord(),
                new JdbcSinkConnectorConfig(properties).cloudEventsSchemaNamePattern());
        consume(convertedRecord);

        final String destinationTableName = destinationTableName(convertedRecord);

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName);
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "Jane Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName, "id");
    }

    protected void assertHasPrimaryKeyColumns(String tableName, String... columnNames) {
        assertHasPrimaryKeyColumns(tableName, true, columnNames);
    }

    protected void assertHasPrimaryKeyColumns(String tableName, boolean caseInsensitive, String... columnNames) {
        List<String> pkColumnNames = TestHelper.getPrimaryKeyColumnNames(dataSource(), tableName);
        if (columnNames.length == 0) {
            assertThat(pkColumnNames).isEmpty();
        }
        else if (caseInsensitive) {
            pkColumnNames = pkColumnNames.stream().map(String::toLowerCase).collect(Collectors.toList());
            for (int columnIndex = 0; columnIndex < columnNames.length; ++columnIndex) {
                assertThat(pkColumnNames).contains(columnNames[columnIndex].toLowerCase(), Index.atIndex(columnIndex));
            }
        }
        else {
            // noinspection ConfusingArgumentToVarargsMethod
            assertThat(pkColumnNames).containsExactly(columnNames);
        }
    }

}
