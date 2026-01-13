/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

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
 * Test demonstrating UNNEST behavior with duplicate primary keys.
 * Proves that UNNEST behaves like standard batching with reWriteBatchedInserts=true:
 * - UNNEST: Always ONE SQL statement (duplicates fail)
 * - Standard batching without reWrite: Multiple statements (duplicates OK)
 * - Standard batching with reWrite: ONE SQL statement (duplicates fail)
 *
 * @author Gaurav Miglani
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkUnnestBehaviorIT extends AbstractJdbcSinkTest {

    public JdbcSinkUnnestBehaviorIT(Sink sink) {
        super(sink);
    }

    /**
     * Test Case 1: reduction=true, unnest=false
     * Expected: PASS - No duplicates reach writer, standard batching
     */
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-1525")
    public void testUpsertWithReductionBufferAndStandardBatching(SinkRecordFactory factory) throws SQLException {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.USE_REDUCTION_BUFFER, "true"); // Merges duplicates
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "false");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        // Create 3 records with duplicate key "1"
        final KafkaDebeziumSinkRecord record1 = factory.createRecord(topicName, (byte) 1); // id=1, value=1
        final KafkaDebeziumSinkRecord record2 = factory.createRecord(topicName, (byte) 1); // id=1, value=2 (duplicate!)
        final KafkaDebeziumSinkRecord record3 = factory.createRecord(topicName, (byte) 2); // id=2, value=3

        // Pass all records in one call to ensure proper batching
        consume(java.util.List.of(record1, record2, record3));

        // Reduction buffer merges record1+record2 → only 2 records inserted
        final var tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.hasNumberOfRows(2);
    }

    /**
     * Test Case 2: reduction=true, unnest=true
     * Expected: PASS - No duplicates, UNNEST works perfectly
     */
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-1525")
    public void testUpsertWithReductionBufferAndUnnestOptimization(SinkRecordFactory factory) throws SQLException {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.USE_REDUCTION_BUFFER, "true"); // Merges duplicates
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "true"); // UNNEST enabled

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        // Create 3 records with duplicate key "1"
        final KafkaDebeziumSinkRecord record1 = factory.createRecord(topicName, (byte) 1);
        final KafkaDebeziumSinkRecord record2 = factory.createRecord(topicName, (byte) 1); // duplicate!
        final KafkaDebeziumSinkRecord record3 = factory.createRecord(topicName, (byte) 2);

        // Pass all records in one call to ensure proper batching
        consume(java.util.List.of(record1, record2, record3));

        // Reduction buffer merges → UNNEST executes successfully
        final var tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.hasNumberOfRows(2);
    }

    /**
     * Test Case 3: reduction=false, unnest=false
     * Expected: PASS - Standard JDBC batching handles duplicates (each statement executes separately)
     */
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-1525")
    public void testUpsertWithoutReductionBufferAndStandardBatching(SinkRecordFactory factory) throws SQLException {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.USE_REDUCTION_BUFFER, "false"); // No merging
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "false");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        // Create 3 records with duplicate key "1"
        final KafkaDebeziumSinkRecord record1 = factory.createRecord(topicName, (byte) 1); // Insert id=1
        final KafkaDebeziumSinkRecord record2 = factory.createRecord(topicName, (byte) 1); // Update id=1 (duplicate!)
        final KafkaDebeziumSinkRecord record3 = factory.createRecord(topicName, (byte) 2); // Insert id=2

        // Pass all records in one call to ensure proper batching
        consume(java.util.List.of(record1, record2, record3));

        // Standard batching: Each INSERT executes separately
        // - Statement 1: INSERT id=1 → success
        // - Statement 2: INSERT id=1 ON CONFLICT UPDATE → success (updates)
        // - Statement 3: INSERT id=2 → success
        final var tableAssert = TestHelper.assertTable(assertDbConnection(), destinationTableName(record1));
        tableAssert.hasNumberOfRows(2); // id=1 (updated) and id=2
    }

    /**
     * Test Case 4: reduction=false, unnest=true
     * Demonstrates that UNNEST with duplicate keys causes PostgreSQL error:
     * "ON CONFLICT DO UPDATE command cannot affect row a second time"
     *
     * This proves UNNEST behaves like reWriteBatchedInserts=true (single statement with duplicates fails).
     * Users should use reduction=true (default) to avoid this error.
     */
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-1525")
    public void testUpsertWithoutReductionBufferAndUnnestCausesDuplicateKeyError(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.USE_REDUCTION_BUFFER, "false"); // No merging
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "true"); // UNNEST enabled

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        // Create 3 records with duplicate key "1"
        final KafkaDebeziumSinkRecord record1 = factory.createRecord(topicName, (byte) 1);
        final KafkaDebeziumSinkRecord record2 = factory.createRecord(topicName, (byte) 1); // duplicate!
        final KafkaDebeziumSinkRecord record3 = factory.createRecord(topicName, (byte) 2);

        // UNNEST generates ONE SQL statement with all 3 records:
        // INSERT INTO t SELECT * FROM UNNEST(ARRAY[1,1,2], ...) ON CONFLICT (id) DO UPDATE ...
        //
        // PostgreSQL detects duplicate id=1 within the same statement → ERROR
        assertThatThrownBy(() -> {
            // Pass all records in one call to ensure they're in the same batch
            consume(java.util.List.of(record1, record2, record3));
            // Trigger flush - this is when the exception is thrown
            consume(Collections.emptyList());
        })
                .hasCauseInstanceOf(org.apache.kafka.connect.errors.ConnectException.class)
                .hasRootCauseMessage(
                        "ERROR: ON CONFLICT DO UPDATE command cannot affect row a second time\n  Hint: Ensure that no rows proposed for insertion within the same command have duplicate constrained values.");

        stopSinkConnector();
    }
}
