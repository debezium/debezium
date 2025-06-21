/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.engine.DebeziumEngine;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.signal.actions.AbstractSnapshotSignal;
import io.debezium.util.Strings;

public abstract class AbstractSnapshotTest<T extends SourceConnector> extends AbstractAsyncEngineConnectorTest {

    protected static final int ROW_COUNT = 1000;
    protected static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-is.txt")
            .toAbsolutePath();
    protected static final int PARTITION_NO = 0;
    protected static final String SERVER_NAME = "test_server";
    private static final int MAXIMUM_NO_RECORDS_CONSUMES = 5;
    protected final Path signalsFile = Paths.get("src", "test", "resources")
            .resolve("debezium_signaling_file.txt");

    protected abstract Class<T> connectorClass();

    protected abstract JdbcConnection databaseConnection();

    protected abstract String topicName();

    protected abstract String tableName();

    protected abstract List<String> topicNames();

    protected abstract List<String> tableNames();

    protected abstract String signalTableName();

    protected String signalTableNameSanitized() {
        return signalTableName();
    }

    protected abstract Configuration.Builder config();

    protected abstract Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl);

    protected abstract String connector();

    protected abstract String server();

    protected String task() {
        return null;
    }

    protected String database() {
        return null;
    }

    protected void waitForCdcTransactionPropagation(int expectedTransactions) throws Exception {
    }

    protected String alterTableAddColumnStatement(String tableName) {
        return "ALTER TABLE " + tableName + " add col3 int default 0";
    }

    protected String alterTableDropColumnStatement(String tableName) {
        return "ALTER TABLE " + tableName + " drop column col3";
    }

    protected String tableDataCollectionId() {
        return tableName();
    }

    protected List<String> tableDataCollectionIds() {
        return tableNames();
    }

    protected void populateTable(JdbcConnection connection, String tableName) throws SQLException {
        connection.setAutoCommit(false);
        for (int i = 0; i < ROW_COUNT; i++) {
            connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                    tableName, connection.quoteIdentifier(pkFieldName()), i + 1, i));
        }
        connection.commit();
    }

    protected void populateTable(JdbcConnection connection) throws SQLException {
        populateTable(connection, tableName());
    }

    protected void populateTables(JdbcConnection connection) throws SQLException {
        for (String tableName : tableNames()) {
            populateTable(connection, tableName);
        }
    }

    protected void populateTable() throws SQLException {
        try (JdbcConnection connection = databaseConnection()) {
            populateTable(connection);
        }
    }

    protected void populateTable(String table) throws SQLException {
        try (JdbcConnection connection = databaseConnection()) {
            populateTable(connection, table);
        }
    }

    protected void populateTableWithSpecificValue(int startRow, int count, int value) throws SQLException {
        try (JdbcConnection connection = databaseConnection()) {
            populateTableWithSpecificValue(connection, tableName(), startRow, count, value);
        }
    }

    private void populateTableWithSpecificValue(JdbcConnection connection, String tableName, int startRow, int count, int value)
            throws SQLException {
        connection.setAutoCommit(false);
        for (int i = startRow + 1; i <= startRow + count; i++) {
            connection.executeWithoutCommitting(
                    String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                            tableName, connection.quoteIdentifier(pkFieldName()), count + i, value));
        }
        connection.commit();
    }

    protected void populateTables() throws SQLException {
        try (JdbcConnection connection = databaseConnection()) {
            populateTables(connection);
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
        return consumeMixedWithIncrementalSnapshot(recordCount, topicName());
    }

    protected Map<Integer, Integer> consumeMixedWithIncrementalSnapshot(int recordCount, Consumer<List<SourceRecord>> recordConsumer) throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()), x -> true,
                recordConsumer, topicName());
    }

    protected Map<Integer, Integer> consumeMixedWithIncrementalSnapshot(int recordCount, String topicName) throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()), x -> true, null,
                topicName);
    }

    protected <V> Map<Integer, V> consumeMixedWithIncrementalSnapshot(int recordCount, Function<SourceRecord, V> valueConverter,
                                                                      Predicate<Map.Entry<Integer, V>> dataCompleted,
                                                                      Consumer<List<SourceRecord>> recordConsumer,
                                                                      String topicName)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, dataCompleted, k -> k.getInt32(pkFieldName()), valueConverter, topicName, recordConsumer);
    }

    protected <V> Map<Integer, V> consumeMixedWithIncrementalSnapshot(int recordCount,
                                                                      Predicate<Map.Entry<Integer, V>> dataCompleted,
                                                                      Function<Struct, Integer> idCalculator,
                                                                      Function<SourceRecord, V> valueConverter,
                                                                      String topicName,
                                                                      Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, dataCompleted, idCalculator, valueConverter, topicName, recordConsumer, true);
    }

    protected <V> Map<Integer, V> consumeMixedWithIncrementalSnapshot(int recordCount,
                                                                      Predicate<Map.Entry<Integer, V>> dataCompleted,
                                                                      Function<Struct, Integer> idCalculator,
                                                                      Function<SourceRecord, V> valueConverter,
                                                                      String topicName,
                                                                      Consumer<List<SourceRecord>> recordConsumer,
                                                                      boolean assertRecords)
            throws InterruptedException {
        final Map<Integer, V> dbChanges = new HashMap<>();
        int noRecords = 0;
        for (;;) {
            final SourceRecords records = consumeRecordsByTopic(1, assertRecords);
            final List<SourceRecord> dataRecords = records.recordsForTopic(topicName);
            if (records.allRecordsInOrder().isEmpty()) {
                noRecords++;
                assertThat(noRecords).describedAs(String.format("Too many no data record results, %d < %d", dbChanges.size(), recordCount))
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

        assertThat(dbChanges).hasSize(recordCount);
        return dbChanges;
    }

    protected Map<Integer, SourceRecord> consumeRecordsMixedWithIncrementalSnapshot(int recordCount) throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, Function.identity(), x -> true, null, topicName());
    }

    protected Map<Integer, Integer> consumeMixedWithIncrementalSnapshot(int recordCount, Predicate<Map.Entry<Integer, Integer>> dataCompleted,
                                                                        Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()), dataCompleted,
                recordConsumer, topicName());
    }

    protected Map<Integer, SourceRecord> consumeRecordsMixedWithIncrementalSnapshot(int recordCount, Predicate<Map.Entry<Integer, SourceRecord>> dataCompleted,
                                                                                    Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, Function.identity(), dataCompleted, recordConsumer, topicName());
    }

    protected String valueFieldName() {
        return "aa";
    }

    protected String pkFieldName() {
        return "pk";
    }

    protected void startConnector(DebeziumEngine.CompletionCallback callback) {
        startConnector(Function.identity(), callback, true);
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        startConnector(custConfig, loggingCompletion(), true);
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig,
                                  DebeziumEngine.CompletionCallback callback, boolean expectNoRecords) {
        final Configuration config = custConfig.apply(config()).build();
        start(connectorClass(), config, callback);
        waitForConnectorToStart();

        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        if (expectNoRecords) {
            // there shouldn't be any snapshot records
            assertNoRecordsToConsume();
        }
    }

    protected void startConnectorWithSnapshot(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        startConnector(custConfig, loggingCompletion(), false);
    }

    protected void startConnector() {
        startConnector(Function.identity(), loggingCompletion(), true);
    }

    protected void waitForConnectorToStart() {
        assertConnectorIsRunning();
    }

    protected Function<Struct, Integer> getRecordValue() {
        return s -> s.getStruct("after").getInt32(valueFieldName());
    }

    @Override
    protected int getMaximumEnqueuedRecordCount() {
        return ROW_COUNT * 3;
    }

    protected void sendExecuteSnapshotFileSignal(String fullTableNames) throws IOException {

        sendExecuteSnapshotFileSignal(fullTableNames, "INCREMENTAL", signalsFile);

    }

    protected void sendExecuteSnapshotFileSignal(String fullTableNames, String type, Path signalFile) throws IOException {

        String signalValue = String.format(
                "{\"id\":\"12345\",\"type\":\"execute-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"%s\"}}",
                fullTableNames, type);

        java.nio.file.Files.write(signalFile, signalValue.getBytes());

    }

    protected void sendAdHocSnapshotSignal(String... dataCollectionIds) throws SQLException {
        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey("", "", dataCollectionIds);
    }

    protected void sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(String additionalCondition, String surrogateKey,
                                                                                  String... dataCollectionIds) {
        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(additionalCondition, surrogateKey, AbstractSnapshotSignal.SnapshotType.INCREMENTAL,
                dataCollectionIds);
    }

    protected void sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(Map<String, String> additionalConditions, String surrogateKey,
                                                                                   String... dataCollectionIds) {
        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(additionalConditions, surrogateKey, AbstractSnapshotSignal.SnapshotType.INCREMENTAL,
                dataCollectionIds);
    }

    protected void sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(String additionalCondition, String surrogateKey,
                                                                                  AbstractSnapshotSignal.SnapshotType snapshotType,
                                                                                  String... dataCollectionIds) {
        final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                .map(x -> '"' + x + '"')
                .collect(Collectors.joining(", "));
        try (JdbcConnection connection = databaseConnection()) {
            String query;
            if (!Strings.isNullOrEmpty(additionalCondition) && !Strings.isNullOrEmpty(surrogateKey)) {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"type\": \"%s\",\"data-collections\": [%s], \"additional-condition\": %s, \"surrogate-key\": %s}')",
                        signalTableName(), snapshotType.toString(), dataCollectionIdsList, additionalCondition, surrogateKey);
            }
            else if (!Strings.isNullOrEmpty(additionalCondition)) {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"type\": \"%s\",\"data-collections\": [%s], \"additional-condition\": %s}')",
                        signalTableName(), snapshotType.toString(), dataCollectionIdsList, additionalCondition);
            }
            else if (!Strings.isNullOrEmpty(surrogateKey)) {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"type\": \"%s\",\"data-collections\": [%s], \"surrogate-key\": %s}')",
                        signalTableName(), snapshotType.toString(), dataCollectionIdsList, surrogateKey);
            }
            else {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"type\": \"%s\",\"data-collections\": [%s]}')",
                        signalTableName(), snapshotType.toString(), dataCollectionIdsList);
            }
            logger.info("Sending signal with query {}", query);
            connection.execute(query);
        }
        catch (Exception e) {
            logger.warn("Failed to send signal", e);
        }
    }

    protected void sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(Map<String, String> additionalConditions, String surrogateKey,
                                                                                   AbstractSnapshotSignal.SnapshotType snapshotType,
                                                                                   String... dataCollectionIds) {
        final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                .map(x -> '"' + x + '"')
                .collect(Collectors.joining(", "));
        try (JdbcConnection connection = databaseConnection()) {
            String query;
            if (!additionalConditions.isEmpty() && !Strings.isNullOrEmpty(surrogateKey)) {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"type\": \"%s\",\"data-collections\": [%s], \"additional-conditions\": [%s], \"surrogate-key\": %s}')",
                        signalTableName(), snapshotType.toString(), dataCollectionIdsList, buildAdditionalConditions(additionalConditions), surrogateKey);
            }
            else if (!additionalConditions.isEmpty()) {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"type\": \"%s\",\"data-collections\": [%s], \"additional-conditions\": [%s]}')",
                        signalTableName(), snapshotType.toString(), dataCollectionIdsList, buildAdditionalConditions(additionalConditions));
            }
            else if (!Strings.isNullOrEmpty(surrogateKey)) {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"type\": \"%s\",\"data-collections\": [%s], \"surrogate-key\": %s}')",
                        signalTableName(), snapshotType.toString(), dataCollectionIdsList, surrogateKey);
            }
            else {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"type\": \"%s\",\"data-collections\": [%s]}')",
                        signalTableName(), snapshotType.toString(), dataCollectionIdsList);
            }
            logger.info("Sending signal with query {}", query);
            connection.execute(query);
        }
        catch (Exception e) {
            logger.warn("Failed to send signal", e);
        }
    }

    protected static String buildAdditionalConditions(Map<String, String> additionalConditions) {

        return additionalConditions.entrySet().stream()
                .map(cond -> String.format("{\"data-collection\": \"%s\", \"filter\": \"%s\"}", cond.getKey(), cond.getValue()))
                .collect(Collectors.joining(","));
    }
}
