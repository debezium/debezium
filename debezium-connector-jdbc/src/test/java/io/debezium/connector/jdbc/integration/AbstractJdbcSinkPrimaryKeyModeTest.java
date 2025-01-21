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
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
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
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Common primary key mode tests.
 *
 * @author Chris Cranford
 */
public abstract class AbstractJdbcSinkPrimaryKeyModeTest extends AbstractJdbcSinkTest {

    public AbstractJdbcSinkPrimaryKeyModeTest(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithNoPrimaryKeyColumnsWithPrimaryKeyModeNone(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithNoPrimaryKeyColumnsWithPrimaryKeyModeKafka(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.KAFKA.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(6);

        getSink().assertColumnType(tableAssert, "__connect_topic", ValueType.TEXT, topicName);
        getSink().assertColumnType(tableAssert, "__connect_partition", ValueType.NUMBER, 0);
        getSink().assertColumnType(tableAssert, "__connect_offset", ValueType.NUMBER, 0);
        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName, "__connect_topic", "__connect_partition", "__connect_offset");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithPrimaryKeyColumnWithPrimaryKeyModeKafka(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.KAFKA.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecord(topicName);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(6);

        getSink().assertColumnType(tableAssert, "__connect_topic", ValueType.TEXT, topicName);
        getSink().assertColumnType(tableAssert, "__connect_partition", ValueType.NUMBER, 0);
        getSink().assertColumnType(tableAssert, "__connect_offset", ValueType.NUMBER, 1L);
        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName, "__connect_topic", "__connect_partition", "__connect_offset");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithPrimaryKeyColumnWithPrimaryKeyModeRecordKey(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecord(topicName);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");

        TestHelper.assertTable(dataSource(), destinationTableName)
                .exists()
                .hasNumberOfColumns(3)
                .column("id").isNumber(false).hasValues((byte) 1)
                .column("name").isText(false).hasValues("John Doe")
                .column("nick_name$").isText(false).hasValues("John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName, "id");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithPrimaryKeyColumnsWithPrimaryKeyModeRecordKey(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordMultipleKeyColumns(topicName);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id1", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "id2", ValueType.NUMBER, 10);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");

        assertHasPrimaryKeyColumns(destinationTableName, "id1", "id2");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithPrimaryKeyColumnWithPrimaryKeyModeRecordHeader(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_HEADER.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        KafkaDebeziumSinkRecord createRecord = factory.createRecord(topicName);
        createRecord.getOriginalKafkaRecord().headers().addInt("id", 1);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");

        TestHelper.assertTable(dataSource(), destinationTableName)
                .exists()
                .hasNumberOfColumns(3)
                .column("id").isNumber(false).hasValues((byte) 1)
                .column("name").isText(false).hasValues("John Doe")
                .column("nick_name$").isText(false).hasValues("John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName, "id");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithPrimaryKeyColumnsWithPrimaryKeyModeRecordHeader(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_HEADER.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        KafkaDebeziumSinkRecord createRecord = factory.createRecordMultipleKeyColumns(topicName);
        SinkRecord kafkaSinkRecord = new SinkRecord(createRecord.topicName(), createRecord.partition(), null, null, createRecord.valueSchema(), createRecord.value(),
                createRecord.offset());
        kafkaSinkRecord.headers().addInt("id1", 1);
        kafkaSinkRecord.headers().addInt("id2", 10);
        KafkaDebeziumSinkRecord kafkaSinkRecordWithHeader = new KafkaDebeziumSinkRecord(kafkaSinkRecord);
        consume(kafkaSinkRecordWithHeader);

        final String destinationTableName = destinationTableName(kafkaSinkRecordWithHeader);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id1", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "id2", ValueType.NUMBER, 10);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");

        assertHasPrimaryKeyColumns(destinationTableName, "id1", "id2");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithNoPrimaryKeyColumnsWithPrimaryKeyModeRecordValue(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id,name");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName, "id", "name");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithPrimaryKeyColumnWithPrimaryKeyModeRecordValueWithNoFieldsSpecified(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        final KafkaDebeziumSinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName, "id", "name", "nick_name$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithPrimaryKeyColumnWithPrimaryKeyModeRecordValue(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id,name");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecord(topicName);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$");

        assertHasPrimaryKeyColumns(destinationTableName, "id", "name");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithPrimaryKeyColumnsWithPrimaryKeyModeRecordValue(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id1,id2,name");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordMultipleKeyColumns(topicName);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id1", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "id2", ValueType.NUMBER, 10);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");

        assertHasPrimaryKeyColumns(destinationTableName, "id1", "id2", "name");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithPrimaryKeyColumnWithPrimaryKeyModeRecordValueAndReductionBuffer(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.USE_REDUCTION_BUFFER, "true");
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id1_value,id2_value");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord1 = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("id1_value", "id2_value", "name"),
                List.of(SchemaBuilder.type(Schema.INT8_SCHEMA.type()).optional().build(),
                        SchemaBuilder.type(Schema.INT8_SCHEMA.type()).optional().build(),
                        SchemaBuilder.type(Schema.STRING_SCHEMA.type()).optional().build()),
                Arrays.asList((byte) 11, (byte) 22, "John Doe 1"));

        final KafkaDebeziumSinkRecord createRecord2 = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("id1_value", "id2_value", "name"),
                List.of(SchemaBuilder.type(Schema.INT8_SCHEMA.type()).optional().build(),
                        SchemaBuilder.type(Schema.INT8_SCHEMA.type()).optional().build(),
                        SchemaBuilder.type(Schema.STRING_SCHEMA.type()).optional().build()),
                Arrays.asList((byte) 11, (byte) 22, "John Doe 2"));

        consume(List.of(createRecord1, createRecord2));

        final String destinationTableName = destinationTableName(createRecord1);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(4);

        getSink().assertColumnType(tableAssert, "id1_value", ValueType.NUMBER, (byte) 11);
        getSink().assertColumnType(tableAssert, "id2_value", ValueType.NUMBER, (byte) 22);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe 2");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testRecordWithPrimaryKeyColumnsWithPrimaryKeyModeRecordValueWithSubsetOfFields(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id1,name");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordMultipleKeyColumns(topicName);
        consume(createRecord);

        final String destinationTableName = destinationTableName(createRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id1", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "id2", ValueType.NUMBER, 10);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe");

        assertHasPrimaryKeyColumns(destinationTableName, "id1", "name");
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
            assertThat(pkColumnNames.size()).isEqualTo(columnNames.length);
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
