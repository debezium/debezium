/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.fest.assertions.MapAssert;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.engine.DebeziumEngine;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

public abstract class AbstractIncrementalSnapshotTest<T extends SourceConnector> extends AbstractConnectorTest {

    protected static final int ROW_COUNT = 1_000;
    private static final int MAXIMUM_NO_RECORDS_CONSUMES = 3;

    protected static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-is.txt")
            .toAbsolutePath();

    protected abstract Class<T> connectorClass();

    protected abstract JdbcConnection databaseConnection();

    protected abstract String topicName();

    protected abstract String tableName();

    protected abstract String signalTableName();

    protected abstract Configuration.Builder config();

    protected String tableDataCollectionId() {
        return tableName();
    }

    protected void populateTable(JdbcConnection connection, String tableName) throws SQLException {
        connection.setAutoCommit(false);
        for (int i = 0; i < ROW_COUNT; i++) {
            connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk, aa) VALUES (%s, %s)", tableName, i + 1, i));
        }
        connection.commit();
    }

    protected void populateTable(JdbcConnection connection) throws SQLException {
        populateTable(connection, tableName());
    }

    protected void populateTable() throws SQLException {
        try (final JdbcConnection connection = databaseConnection()) {
            populateTable(connection);
        }
    }

    protected void populate4PkTable(JdbcConnection connection, String tableName) throws SQLException {
        connection.setAutoCommit(false);
        for (int i = 0; i < ROW_COUNT; i++) {
            final int id = i + 1;
            final int pk1 = id / 1000;
            final int pk2 = (id / 100) % 10;
            final int pk3 = (id / 10) % 10;
            final int pk4 = id % 10;
            connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk1, pk2, pk3, pk4, aa) VALUES (%s, %s, %s, %s, %s)",
                    tableName,
                    pk1,
                    pk2,
                    pk3,
                    pk4,
                    i));
        }
        connection.commit();
    }

    protected Map<Integer, Integer> consumeMixedWithIncrementalSnapshot(int recordCount) throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()), x -> true, null);
    }

    protected <V> Map<Integer, V> consumeMixedWithIncrementalSnapshot(int recordCount, Function<SourceRecord, V> valueConverter,
                                                                      Predicate<Map.Entry<Integer, V>> dataCompleted,
                                                                      Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, dataCompleted, k -> k.getInt32(pkFieldName()), valueConverter, topicName(), recordConsumer);
    }

    protected <V> Map<Integer, V> consumeMixedWithIncrementalSnapshot(int recordCount,
                                                                      Predicate<Map.Entry<Integer, V>> dataCompleted,
                                                                      Function<Struct, Integer> idCalculator,
                                                                      Function<SourceRecord, V> valueConverter,
                                                                      String topicName,
                                                                      Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        final Map<Integer, V> dbChanges = new HashMap<>();
        int noRecords = 0;
        for (;;) {
            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SourceRecord> dataRecords = records.recordsForTopic(topicName);
            if (records.allRecordsInOrder().isEmpty()) {
                noRecords++;
                Assertions.assertThat(noRecords).describedAs(String.format("Too many no data record results, %d < %d", dbChanges.size(), recordCount))
                        .isLessThanOrEqualTo(MAXIMUM_NO_RECORDS_CONSUMES);
                continue;
            }
            noRecords = 0;
            if (dataRecords == null || dataRecords.isEmpty()) {
                continue;
            }
            dataRecords.forEach(record -> {
                final int id = idCalculator.apply((Struct) record.key());
                final V value = valueConverter.apply(record);
                dbChanges.put(id, value);
            });
            if (recordConsumer != null) {
                recordConsumer.accept(dataRecords);
            }
            if (dbChanges.size() >= recordCount) {
                if (!dbChanges.entrySet().stream().anyMatch(dataCompleted.negate())) {
                    break;
                }
            }
        }

        Assertions.assertThat(dbChanges).hasSize(recordCount);
        return dbChanges;
    }

    protected Map<Integer, SourceRecord> consumeRecordsMixedWithIncrementalSnapshot(int recordCount) throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, Function.identity(), x -> true, null);
    }

    protected Map<Integer, Integer> consumeMixedWithIncrementalSnapshot(int recordCount, Predicate<Map.Entry<Integer, Integer>> dataCompleted,
                                                                        Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()), dataCompleted,
                recordConsumer);
    }

    protected Map<Integer, SourceRecord> consumeRecordsMixedWithIncrementalSnapshot(int recordCount, Predicate<Map.Entry<Integer, SourceRecord>> dataCompleted,
                                                                                    Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, Function.identity(), dataCompleted, recordConsumer);
    }

    protected String valueFieldName() {
        return "aa";
    }

    protected String pkFieldName() {
        return "pk";
    }

    protected void sendAdHocSnapshotSignal(String... dataCollectionIds) throws SQLException {
        final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                .map(x -> '"' + x + '"')
                .collect(Collectors.joining(", "));
        try (final JdbcConnection connection = databaseConnection()) {
            String query = String.format(
                    "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"data-collections\": [%s]}')",
                    signalTableName(), dataCollectionIdsList);
            logger.info("Sending signal with query {}", query);
            connection.execute(query);
        }
        catch (Exception e) {
            logger.warn("Failed to send signal", e);
        }
    }

    protected void sendAdHocSnapshotSignal() throws SQLException {
        sendAdHocSnapshotSignal(tableDataCollectionId());
    }

    protected void startConnector(DebeziumEngine.CompletionCallback callback) {
        startConnector(Function.identity(), callback);
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        startConnector(custConfig, loggingCompletion());
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig, DebeziumEngine.CompletionCallback callback) {
        final Configuration config = custConfig.apply(config()).build();
        start(connectorClass(), config, callback);
        waitForConnectorToStart();

        waitForAvailableRecords(5, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();
    }

    protected void startConnector() {
        startConnector(Function.identity(), loggingCompletion());
    }

    protected void waitForConnectorToStart() {
        assertConnectorIsRunning();
    }

    @Test
    public void snapshotOnly() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test
    public void invalidTablesInTheList() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal("invalid1", tableDataCollectionId(), "invalid2");

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test
    public void inserts() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal();

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk, aa) VALUES (%s, %s)",
                        tableName(),
                        i + ROW_COUNT + 1,
                        i + ROW_COUNT));
            }
            connection.commit();
        }

        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test
    public void updates() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal();

        final int batchSize = 10;
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(
                        String.format("UPDATE %s SET aa = aa + 2000 WHERE pk > %s AND pk <= %s", tableName(),
                                i * batchSize, (i + 1) * batchSize));
                connection.commit();
            }
        }

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount,
                x -> x.getValue() >= 2000, null);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i + 2000));
        }
    }

    @Test
    public void updatesWithRestart() throws Exception {
        // Testing.Print.enable();

        populateTable();
        final Configuration config = config().build();
        startAndConsumeTillEnd(connectorClass(), config);
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignal();

        final int batchSize = 10;
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(
                        String.format("UPDATE %s SET aa = aa + 2000 WHERE pk > %s AND pk <= %s", tableName(),
                                i * batchSize, (i + 1) * batchSize));
                connection.commit();
            }
        }

        final int expectedRecordCount = ROW_COUNT;
        final AtomicInteger recordCounter = new AtomicInteger();
        final AtomicBoolean restarted = new AtomicBoolean();
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount,
                x -> x.getValue() >= 2000, x -> {
                    if (recordCounter.addAndGet(x.size()) > 50 && !restarted.get()) {
                        stopConnector();
                        assertConnectorNotRunning();

                        start(connectorClass(), config);
                        waitForConnectorToStart();
                        restarted.set(true);
                    }
                });
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i + 2000));
        }
    }

    @Test
    public void updatesLargeChunk() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector(x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, ROW_COUNT));

        sendAdHocSnapshotSignal();

        try (JdbcConnection connection = databaseConnection()) {
            connection.execute(String.format("UPDATE %s SET aa = aa + 2000", tableName()));
        }

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount,
                x -> x.getValue() >= 2000, null);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i + 2000));
        }
    }

    @Test
    public void snapshotOnlyWithRestart() throws Exception {
        // Testing.Print.enable();

        populateTable();
        final Configuration config = config().build();
        startAndConsumeTillEnd(connectorClass(), config);
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final AtomicInteger recordCounter = new AtomicInteger();
        final AtomicBoolean restarted = new AtomicBoolean();
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount, x -> true,
                x -> {
                    if (recordCounter.addAndGet(x.size()) > 50 && !restarted.get()) {
                        stopConnector();
                        assertConnectorNotRunning();

                        start(connectorClass(), config);
                        waitForConnectorToStart();
                        restarted.set(true);
                    }
                });
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Override
    protected int getMaximumEnqueuedRecordCount() {
        return ROW_COUNT * 3;
    }
}
