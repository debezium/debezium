/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Integration tests for the shared sink framework buffer modes
 * ({@code keyed.message.batch.mode=passthrough|deduplication}).
 * <p>
 * These tests require {@code -Denable.sces=true} to run against the shared sink framework.
 *
 * @author rk3rn3r
 */
public abstract class AbstractJdbcSinkBufferModeTest extends AbstractJdbcSinkTest {

    public AbstractJdbcSinkBufferModeTest(Sink sink) {
        super(sink);
    }

    @BeforeEach
    public void requireSharedSinkFramework() {
        Assumptions.assumeTrue("true".equals(System.getProperty("enable.sces")),
                "Buffer mode tests require -Denable.sces=true");
    }

    // -----------------------------------------------------------------------
    // KeylessPassthroughBuffer — verifies all keyless events are preserved
    // -----------------------------------------------------------------------

    @FixFor("debezium/dbz#1185")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testKeylessPassthroughPreservesAllRecords(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);

        final JdbcKafkaSinkRecord record1 = factory.createRecordNoKey(topicName, config);
        final JdbcKafkaSinkRecord record2 = factory.createRecordNoKey(topicName, config);
        final JdbcKafkaSinkRecord record3 = factory.createRecordNoKey(topicName, config);

        consume(List.of(record1, record2, record3));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.exists().hasNumberOfRows(3).hasNumberOfColumns(3);
    }

    @FixFor("debezium/dbz#1185")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testKeylessPassthroughPreservesIdenticalRecords(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);

        final JdbcKafkaSinkRecord record1 = createRecordWithName(factory, topicName, (byte) 1, "Alice", config);
        final JdbcKafkaSinkRecord record2 = createRecordWithName(factory, topicName, (byte) 1, "Alice", config);

        consume(List.of(record1, record2));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "Alice", "Alice");
    }

    @FixFor("debezium/dbz#1185")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testKeylessPassthroughPreservesRecordsWithDifferentValues(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);

        final JdbcKafkaSinkRecord record1 = createRecordWithName(factory, topicName, (byte) 1, "Alice", config);
        final JdbcKafkaSinkRecord record2 = createRecordWithName(factory, topicName, (byte) 1, "Bob", config);
        final JdbcKafkaSinkRecord record3 = createRecordWithName(factory, topicName, (byte) 2, "Charlie", config);

        consume(List.of(record1, record2, record3));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.exists().hasNumberOfRows(3).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 1, (byte) 2);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "Alice", "Bob", "Charlie");
    }

    // -----------------------------------------------------------------------
    // DeduplicatingBuffer (default) — verifies deduplication via last-write-wins
    // -----------------------------------------------------------------------

    @FixFor("debezium/dbz#1185")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testDeduplicatingBufferCollapsesUpsertWithDuplicateKeysInSingleBatch(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);

        final JdbcKafkaSinkRecord record1 = createRecordWithName(factory, topicName, (byte) 1, "Alice", config);
        final JdbcKafkaSinkRecord record2 = createRecordWithName(factory, topicName, (byte) 1, "Bob", config);
        final JdbcKafkaSinkRecord record3 = createRecordWithName(factory, topicName, (byte) 2, "Charlie", config);

        consume(List.of(record1, record2, record3));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 2);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "Bob", "Charlie");

        assertThat(getBatchCount()).as("DeduplicatingBuffer should produce 1 batch").isEqualTo(1);
        assertThat(getTotalRecordsWritten()).as("Deduplicated to 2 records").isEqualTo(2);
    }

    @FixFor("debezium/dbz#1185")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testDeduplicatingBufferCollapsesSameKeyToSingleRow(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);

        final JdbcKafkaSinkRecord record1 = createRecordWithName(factory, topicName, (byte) 1, "v1", config);
        final JdbcKafkaSinkRecord record2 = createRecordWithName(factory, topicName, (byte) 1, "v2", config);
        final JdbcKafkaSinkRecord record3 = createRecordWithName(factory, topicName, (byte) 1, "v3", config);

        consume(List.of(record1, record2, record3));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "v3");

        assertThat(getBatchCount()).as("DeduplicatingBuffer should produce 1 batch").isEqualTo(1);
        assertThat(getTotalRecordsWritten()).as("Deduplicated 3 same-key records to 1").isEqualTo(1);
    }

    // -----------------------------------------------------------------------
    // KeyedPassthroughBuffer — verifies all events are preserved and applied
    // -----------------------------------------------------------------------

    @FixFor("debezium/dbz#1185")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testKeyedPassthroughUpsertPreservesAllEventsForDuplicateKeys(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        properties.put(SinkConnectorConfig.KEYED_MESSAGE_BATCH_MODE, SinkConnectorConfig.KeyedMessageBatchMode.PASSTHROUGH.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);

        final JdbcKafkaSinkRecord record1 = createRecordWithName(factory, topicName, (byte) 1, "Alice", config);
        final JdbcKafkaSinkRecord record2 = createRecordWithName(factory, topicName, (byte) 2, "Bob", config);
        final JdbcKafkaSinkRecord record3 = createRecordWithName(factory, topicName, (byte) 1, "Alice Updated", config);

        consume(List.of(record1, record2, record3));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 2);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "Alice Updated", "Bob");

        // key=1 duplicate splits into 2 batches: [{key=1,key=2}, {key=1}]
        assertThat(getBatchCount()).as("KeyedPassthroughBuffer should produce 2 batches").isEqualTo(2);
        assertThat(getTotalRecordsWritten()).as("All 3 records written").isEqualTo(3);
    }

    @FixFor("debezium/dbz#1185")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testKeyedPassthroughUpsertAppliesAllEventsInOrder(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        properties.put(SinkConnectorConfig.KEYED_MESSAGE_BATCH_MODE, SinkConnectorConfig.KeyedMessageBatchMode.PASSTHROUGH.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);

        final JdbcKafkaSinkRecord record1 = createRecordWithName(factory, topicName, (byte) 1, "v1", config);
        final JdbcKafkaSinkRecord record2 = createRecordWithName(factory, topicName, (byte) 1, "v2", config);
        final JdbcKafkaSinkRecord record3 = createRecordWithName(factory, topicName, (byte) 1, "v3", config);

        consume(List.of(record1, record2, record3));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "v3");

        // 3 records with same key → 3 batches: [{key=1}], [{key=1}], [{key=1}]
        assertThat(getBatchCount()).as("KeyedPassthroughBuffer should produce 3 batches").isEqualTo(3);
        assertThat(getTotalRecordsWritten()).as("All 3 records written").isEqualTo(3);
    }

    @FixFor("debezium/dbz#1185")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testKeyedPassthroughInsertWithDistinctKeys(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        properties.put(SinkConnectorConfig.KEYED_MESSAGE_BATCH_MODE, SinkConnectorConfig.KeyedMessageBatchMode.PASSTHROUGH.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);

        final JdbcKafkaSinkRecord record1 = createRecordWithName(factory, topicName, (byte) 1, "Alice", config);
        final JdbcKafkaSinkRecord record2 = createRecordWithName(factory, topicName, (byte) 2, "Bob", config);
        final JdbcKafkaSinkRecord record3 = createRecordWithName(factory, topicName, (byte) 3, "Charlie", config);

        consume(List.of(record1, record2, record3));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.exists().hasNumberOfRows(3).hasNumberOfColumns(2);

        // No duplicate keys → 1 batch with all 3 records
        assertThat(getBatchCount()).as("No duplicates should produce 1 batch").isEqualTo(1);
        assertThat(getTotalRecordsWritten()).as("All 3 records written").isEqualTo(3);
    }

    @FixFor("debezium/dbz#1185")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testKeyedPassthroughWithMultipleBatchesAndMixedKeys(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        properties.put(SinkConnectorConfig.KEYED_MESSAGE_BATCH_MODE, SinkConnectorConfig.KeyedMessageBatchMode.PASSTHROUGH.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);

        final JdbcKafkaSinkRecord r1 = createRecordWithName(factory, topicName, (byte) 1, "A1", config);
        final JdbcKafkaSinkRecord r2 = createRecordWithName(factory, topicName, (byte) 2, "B1", config);
        final JdbcKafkaSinkRecord r3 = createRecordWithName(factory, topicName, (byte) 1, "A2", config);
        final JdbcKafkaSinkRecord r4 = createRecordWithName(factory, topicName, (byte) 2, "B2", config);
        final JdbcKafkaSinkRecord r5 = createRecordWithName(factory, topicName, (byte) 1, "A3", config);

        consume(List.of(r1, r2, r3, r4, r5));

        final TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(r1));
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(2);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 2);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "A3", "B2");

        // keys 1,2,1,2,1 → flush at r3 (dup key=1): [{1,2}], then r4 (dup key=2 is NOT dup yet, only key=1 in map),
        // wait r4 adds key=2, r5 (dup key=1): flush [{1,2}], remaining [{1}] → 3 batches, 5 records
        assertThat(getBatchCount()).as("Mixed keys should produce 3 batches").isEqualTo(3);
        assertThat(getTotalRecordsWritten()).as("All 5 records written").isEqualTo(5);
    }

    // -----------------------------------------------------------------------
    // Helper methods
    // -----------------------------------------------------------------------

    private JdbcKafkaSinkRecord createRecordWithName(SinkRecordFactory factory, String topicName, byte key, String name,
                                                     JdbcSinkConnectorConfig config) {
        return factory.createRecordWithSchemaValue(
                topicName,
                key,
                List.of("name"),
                List.of(SchemaBuilder.type(Schema.STRING_SCHEMA.type()).optional().build()),
                Collections.singletonList(name),
                config);
    }
}
