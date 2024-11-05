/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.fest.assertions.Index;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.transforms.ConvertCloudEventToSaveableForm;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.converters.spi.SerializerType;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Common converted CloudEvent saving tests.
 *
 * @author Roman Kudryashov
 */
public abstract class AbstractJdbcSinkSaveConvertedCloudEventTest extends AbstractJdbcSinkTest {

    public AbstractJdbcSinkSaveConvertedCloudEventTest(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testSaveConvertedCloudEventRecordFromJson(SinkRecordFactory factory) {
        final ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm();
        final Map<String, String> config = new HashMap<>();
        config.put("fields.mapping", "id,source:created_by,data:payload");
        config.put("serializer.type", "json");
        transform.configure(config);

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord cloudEventRecord = factory.cloudEventRecord(topicName, SerializerType.withName("json"), null);
        final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
        consume(convertedRecord);

        final String destinationTableName = destinationTableName(convertedRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "created_by", ValueType.TEXT, "test_ce_source");
        getSink().assertColumnType(tableAssert, "payload", ValueType.TEXT);

        assertHasPrimaryKeyColumns(destinationTableName, "id");

        transform.close();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testSaveConvertedCloudEventRecordFromAvro(SinkRecordFactory factory) {
        final ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm();
        final Map<String, String> config = new HashMap<>();
        config.put("fields.mapping", "id,source:created_by,data:payload");
        config.put("serializer.type", "avro");
        transform.configure(config);

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord cloudEventRecord = factory.cloudEventRecord(topicName, SerializerType.withName("avro"), null);
        final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
        consume(convertedRecord);

        final String destinationTableName = destinationTableName(convertedRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "created_by", ValueType.TEXT, "test_ce_source");
        getSink().assertColumnType(tableAssert, "payload", ValueType.TEXT);

        assertHasPrimaryKeyColumns(destinationTableName, "id");

        transform.close();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testSaveConvertedCloudEventRecordFromAvroWithCloudEventsSchemaCustomName(SinkRecordFactory factory) {
        final ConvertCloudEventToSaveableForm transform = new ConvertCloudEventToSaveableForm();
        final Map<String, String> config = new HashMap<>();
        config.put("fields.mapping", "id,source:created_by,data:payload");
        config.put("serializer.type", "avro");
        config.put("schema.cloudevents.name", "TestCESchemaCustomName");
        transform.configure(config);

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord cloudEventRecord = factory.cloudEventRecord(topicName, SerializerType.withName("avro"), "TestCESchemaCustomName");
        final SinkRecord convertedRecord = transform.apply(cloudEventRecord);
        consume(convertedRecord);

        final String destinationTableName = destinationTableName(convertedRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName);
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.TEXT);
        getSink().assertColumnType(tableAssert, "created_by", ValueType.TEXT, "test_ce_source");
        getSink().assertColumnType(tableAssert, "payload", ValueType.TEXT);

        assertHasPrimaryKeyColumns(destinationTableName, "id");

        transform.close();
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
