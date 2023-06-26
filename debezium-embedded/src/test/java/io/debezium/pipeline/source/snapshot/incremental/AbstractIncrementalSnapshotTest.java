/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.engine.DebeziumEngine;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenConnectorUnderTest;
import io.debezium.junit.SkipWhenConnectorUnderTest.Connector;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.kafka.KafkaCluster;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.signal.actions.snapshotting.StopSnapshot;
import io.debezium.util.Testing;

public abstract class AbstractIncrementalSnapshotTest<T extends SourceConnector> extends AbstractConnectorTest {

    protected static final int ROW_COUNT = 1_000;
    private static final int MAXIMUM_NO_RECORDS_CONSUMES = 5;

    protected static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-is.txt")
            .toAbsolutePath();
    private static final int PARTITION_NO = 0;
    private static final String SERVER_NAME = "test_server";

    protected static KafkaCluster kafka;

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
                    tableName, connection.quotedColumnIdString(pkFieldName()), i + 1, i));
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
                            tableName, connection.quotedColumnIdString(pkFieldName()), count + i, value));
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
        final Map<Integer, V> dbChanges = new HashMap<>();
        int noRecords = 0;
        for (;;) {
            final SourceRecords records = consumeRecordsByTopic(1);
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

    protected String getSignalTypeFieldName() {
        return "type";
    }

    protected void sendAdHocSnapshotSignal(String... dataCollectionIds) throws SQLException {
        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(Optional.empty(), Optional.empty(), dataCollectionIds);
    }

    protected void sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(Optional<String> additionalCondition, Optional<String> surrogateKey,
                                                                                  String... dataCollectionIds) {
        final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                .map(x -> '"' + x + '"')
                .collect(Collectors.joining(", "));
        try (JdbcConnection connection = databaseConnection()) {
            String query;
            if (additionalCondition.isPresent() && surrogateKey.isPresent()) {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"data-collections\": [%s], \"additional-condition\": %s, \"surrogate-key\": %s}')",
                        signalTableName(), dataCollectionIdsList, additionalCondition.get(), surrogateKey.get());
            }
            else if (additionalCondition.isPresent()) {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"data-collections\": [%s], \"additional-condition\": %s}')",
                        signalTableName(), dataCollectionIdsList, additionalCondition.get());
            }
            else if (surrogateKey.isPresent()) {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"data-collections\": [%s], \"surrogate-key\": %s}')",
                        signalTableName(), dataCollectionIdsList, surrogateKey.get());
            }
            else {
                query = String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"data-collections\": [%s]}')",
                        signalTableName(), dataCollectionIdsList);
            }
            logger.info("Sending signal with query {}", query);
            connection.execute(query);
        }
        catch (Exception e) {
            logger.warn("Failed to send signal", e);
        }
    }

    protected void sendAdHocSnapshotStopSignal(String... dataCollectionIds) throws SQLException {
        String collections = "";
        if (dataCollectionIds.length > 0) {
            final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                    .map(x -> '"' + x + '"')
                    .collect(Collectors.joining(", "));
            collections = ",\"data-collections\": [" + dataCollectionIdsList + "]";
        }

        try (JdbcConnection connection = databaseConnection()) {
            String query = String.format(
                    "INSERT INTO %s VALUES('ad-hoc', 'stop-snapshot', '{\"type\": \"INCREMENTAL\"" + collections + "}')",
                    signalTableName());
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

    protected void sendAdHocKafkaSnapshotSignal() throws ExecutionException, InterruptedException {
        sendExecuteSnapshotKafkaSignal(tableDataCollectionId());
    }

    protected void sendExecuteSnapshotKafkaSignal(String fullTableNames) throws ExecutionException, InterruptedException {
        String signalValue = String.format(
                "{\"type\":\"execute-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}}",
                fullTableNames);
        sendKafkaSignal(signalValue);
    }

    protected String getSignalsTopic() {
        return "signals_topic";
    }

    protected void sendKafkaSignal(String signalValue) throws ExecutionException, InterruptedException {
        final ProducerRecord<String, String> executeSnapshotSignal = new ProducerRecord<>(getSignalsTopic(), PARTITION_NO, SERVER_NAME, signalValue);

        final Configuration signalProducerConfig = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokerList())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "signals")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(signalProducerConfig.asProperties())) {
            producer.send(executeSnapshotSignal).get();
        }
    }

    protected void sendPauseSignal() {
        try (JdbcConnection connection = databaseConnection()) {
            String query = String.format("INSERT INTO %s VALUES('test-pause', 'pause-snapshot', '')", signalTableName());
            logger.info("Sending pause signal with query {}", query);
            connection.execute(query);
        }
        catch (Exception e) {
            logger.warn("Failed to send pause signal", e);
        }
    }

    protected void sendResumeSignal() {
        try (JdbcConnection connection = databaseConnection()) {
            String query = String.format("INSERT INTO %s VALUES('test-resume', 'resume-snapshot', '')", signalTableName());
            logger.info("Sending resume signal with query {}", query);
            connection.execute(query);
        }
        catch (Exception e) {
            logger.warn("Failed to send resume signal", e);
        }
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

        waitForAvailableRecords(5, TimeUnit.SECONDS);
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

    @Test
    public void snapshotOnly() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
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
            assertThat(dbChanges).contains(entry(i + 1, i));
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
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableName(),
                        connection.quotedColumnIdString(pkFieldName()),
                        i + ROW_COUNT + 1,
                        i + ROW_COUNT));
            }
            connection.commit();
        }

        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    public void insertsWithKafkaSnapshotSignal() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal();

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableName(),
                        connection.quotedColumnIdString(pkFieldName()),
                        i + ROW_COUNT + 1,
                        i + ROW_COUNT));
            }
            connection.commit();
        }

        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
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
                        String.format("UPDATE %s SET aa = aa + 2000 WHERE %s > %s AND %s <= %s",
                                tableName(),
                                connection.quotedColumnIdString(pkFieldName()),
                                i * batchSize,
                                connection.quotedColumnIdString(pkFieldName()),
                                (i + 1) * batchSize));
                connection.commit();
            }
        }

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount,
                x -> x.getValue() >= 2000, null);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i + 2000));
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
                        String.format("UPDATE %s SET aa = aa + 2000 WHERE %s > %s AND %s <= %s",
                                tableName(),
                                connection.quotedColumnIdString(pkFieldName()),
                                i * batchSize,
                                connection.quotedColumnIdString(pkFieldName()),
                                (i + 1) * batchSize));
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
            assertThat(dbChanges).contains(entry(i + 1, i + 2000));
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
            assertThat(dbChanges).contains(entry(i + 1, i + 2000));
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
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    @FixFor("DBZ-4272")
    // Disabled due to DBZ-4350
    @SkipWhenConnectorUnderTest(check = EqualityCheck.EQUAL, value = Connector.SQL_SERVER)
    @SkipWhenConnectorUnderTest(check = EqualityCheck.EQUAL, value = Connector.DB2)
    public void snapshotPreceededBySchemaChange() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();
        waitForConnectorToStart();

        // Initiate a schema change to the table immediately before the adhoc-snapshot
        // Adds a new column to the table; this column will be dropped later in this test.
        try (JdbcConnection connection = databaseConnection()) {
            connection.execute(alterTableAddColumnStatement(tableName()));
        }

        // Some connectors, such as PostgreSQL won't be notified of the previous schema change
        // until a DML event occurs, but regardless the incremental snapshot should succeed.
        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }

        // Initiate a schema change to the table immediately before the adhoc-snapshot
        // This schema change will drop the previously added column from above.
        try (JdbcConnection connection = databaseConnection()) {
            connection.execute(alterTableDropColumnStatement(tableName()));
        }

        // Some connectors, such as PostgreSQL won't be notified of the previous schema change
        // until a DML event occurs, but regardless the incremental snapshot should succeed.
        sendAdHocSnapshotSignal();

        dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    public void snapshotWithRegexDataCollections() throws Exception {
        populateTable();
        startConnector();
        sendAdHocSnapshotSignal(".*");

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    @FixFor("DBZ-4271")
    public void stopCurrentIncrementalSnapshotWithoutCollectionsAndTakeNewNewIncrementalSnapshotAfterRestart() throws Exception {

        // Testing.Print.enable();

        final LogInterceptor interceptor = new LogInterceptor(AbstractIncrementalSnapshotChangeEventSource.class);

        // We will use chunk size of 1 to have very small batches to guarantee that when we stop
        // we are still within the incremental snapshot rather than it being performed with one
        // round trip to the database
        populateTable();
        startConnector(x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 1));

        // Send ad-hoc start incremental snapshot signal and wait for incremental snapshots to start
        sendAdHocSnapshotSignalAndWait();

        // stop ad-hoc snapshot without specifying any collections, canceling the entire incremental snapshot
        // This waits until we've received the stop signal.
        sendAdHocSnapshotStopSignalAndWait();

        // Consume any residual left-over events after stopping incremental snapshots such as open/close
        // and wait for the stop message in the connector logs
        assertThat(consumeAnyRemainingIncrementalSnapshotEventsAndCheckForStopMessage(
                interceptor, "Stopping incremental snapshot")).isTrue();

        // stop the connector
        stopConnector((r) -> interceptor.clear());

        // restart the connector
        // should start with no available records, should not have any incremental snapshot state
        startConnector();
        assertThat(interceptor.containsMessage("No incremental snapshot in progress")).isTrue();

        sendAdHocSnapshotSignal();

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableName(),
                        connection.quotedColumnIdString(pkFieldName()),
                        i + ROW_COUNT + 1,
                        i + ROW_COUNT));
            }
            connection.commit();
        }

        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    @FixFor("DBZ-4271")
    public void stopCurrentIncrementalSnapshotWithAllCollectionsAndTakeNewNewIncrementalSnapshotAfterRestart() throws Exception {

        // Testing.Print.enable();

        final LogInterceptor interceptor = new LogInterceptor(AbstractIncrementalSnapshotChangeEventSource.class);

        // We will use chunk size of 1 to have very small batches to guarantee that when we stop
        // we are still within the incremental snapshot rather than it being performed with one
        // round trip to the database
        populateTable();
        startConnector(x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 1));

        // Send ad-hoc start incremental snapshot signal and wait for incremental snapshots to start
        sendAdHocSnapshotSignalAndWait();

        // stop ad-hoc snapshot without specifying any collections, canceling the entire incremental snapshot
        // This waits until we've received the stop signal.
        sendAdHocSnapshotStopSignalAndWait(tableDataCollectionId());

        // Consume any residual left-over events after stopping incremental snapshots such as open/close
        // and wait for the stop message in the connector logs
        assertThat(consumeAnyRemainingIncrementalSnapshotEventsAndCheckForStopMessage(
                interceptor, "Removing '[" + tableDataCollectionId() + "]' collections from incremental snapshot")).isTrue();

        // stop the connector
        stopConnector((r) -> interceptor.clear());

        // restart the connector
        // should start with no available records, should not have any incremental snapshot state
        startConnector();
        assertThat(interceptor.containsMessage("No incremental snapshot in progress")).isTrue();

        sendAdHocSnapshotSignal();

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableName(),
                        connection.quotedColumnIdString(pkFieldName()),
                        i + ROW_COUNT + 1,
                        i + ROW_COUNT));
            }
            connection.commit();
        }

        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    @FixFor("DBZ-4271")
    public void removeNotYetCapturedCollectionFromInProgressIncrementalSnapshot() throws Exception {
        final LogInterceptor interceptor = new LogInterceptor(AbstractIncrementalSnapshotChangeEventSource.class);

        // We will use chunk size of 250, this gives us enough granularity with the incremental
        // snapshot to have a couple round trips for the first table but enough table to trigger
        // the removal of the second table before it starts being processed.
        populateTables();
        startConnector(x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 250));

        final List<String> collectionIds = tableDataCollectionIds();
        assertThat(collectionIds).hasSize(2);

        final List<String> tableNames = tableNames();
        assertThat(tableNames).hasSize(2);

        final List<String> topicNames = topicNames();
        assertThat(topicNames).hasSize(2);

        final String collectionIdToRemove = collectionIds.get(1);
        final String tableToSnapshot = tableNames.get(0);
        final String topicToConsume = topicNames.get(0);

        // Send the start signal for all collections and stop for the second collection
        sendAdHocSnapshotSignal(collectionIds.toArray(new String[0]));
        sendAdHocSnapshotStopSignal(collectionIdToRemove);

        // Wait until the stop has been processed, verifying it was removed from the snapshot.
        Awaitility.await().atMost(60, TimeUnit.SECONDS)
                .until(() -> interceptor.containsMessage("Removing '[" + collectionIdToRemove + "]' collections from incremental snapshot"));

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableToSnapshot,
                        connection.quotedColumnIdString(pkFieldName()),
                        i + ROW_COUNT + 1,
                        i + ROW_COUNT));
            }
            connection.commit();
        }

        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount, topicToConsume);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    @FixFor("DBZ-4271")
    public void removeStartedCapturedCollectionFromInProgressIncrementalSnapshot() throws Exception {
        final LogInterceptor interceptor = new LogInterceptor(AbstractIncrementalSnapshotChangeEventSource.class);

        // We will use chunk size of 250, this gives us enough granularity with the incremental
        // snapshot to have a couple round trips for the first table but enough table to trigger
        // the removal of the second table before it starts being processed.
        populateTables();
        startConnector(x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 250));

        final List<String> collectionIds = tableDataCollectionIds();
        assertThat(collectionIds).hasSize(2);

        final List<String> tableNames = tableNames();
        assertThat(tableNames).hasSize(2);

        final List<String> topicNames = topicNames();
        assertThat(topicNames).hasSize(2);

        final String collectionIdToRemove = collectionIds.get(0);
        final String tableToSnapshot = tableNames.get(1);
        final String topicToConsume = topicNames.get(1);

        // Send the start signal for all collections and stop for the second collection
        sendAdHocSnapshotSignal(collectionIds.toArray(new String[0]));
        sendAdHocSnapshotStopSignal(collectionIdToRemove);

        // Wait until the stop has been processed, verifying it was removed from the snapshot.
        Awaitility.await().atMost(60, TimeUnit.SECONDS)
                .until(() -> interceptor.containsMessage("Removing '[" + collectionIdToRemove + "]' collections from incremental snapshot"));

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableToSnapshot,
                        connection.quotedColumnIdString(pkFieldName()),
                        i + ROW_COUNT + 1,
                        i + ROW_COUNT));
            }
            connection.commit();
        }

        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount, topicToConsume);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    @FixFor("DBZ-4834")
    public void shouldSnapshotNewlyAddedTableToIncludeListAfterRestart() throws Exception {
        // Populate the second table
        populateTables();

        // Start connector, wait until we've transitioned to streaming and stop
        // We only specify the signal table here
        startConnectorWithSnapshot(x -> mutableConfig(true, false));
        waitForConnectorToStart();

        SourceRecords snapshotRecords = consumeRecordsByTopic(ROW_COUNT);

        stopConnector();

        // Restart connector, specifying to include the populated tables
        startConnector(x -> mutableConfig(false, false));
        waitForConnectorToStart();

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }

        stopConnector();
    }

    @Test
    public void testPauseDuringSnapshot() throws Exception {
        populateTable();
        startConnector(x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 50));
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignal();

        List<SourceRecord> records = new ArrayList<>();
        String topicName = topicName();
        consumeRecords(100, record -> {
            if (topicName.equalsIgnoreCase(record.topic())) {
                records.add(record);
            }
        });

        sendPauseSignal();

        consumeAvailableRecords(record -> {
            if (topicName.equalsIgnoreCase(record.topic())) {
                records.add(record);
            }
        });
        int beforeResume = records.size();

        sendResumeSignal();

        final int expectedRecordCount = ROW_COUNT;
        if ((expectedRecordCount - beforeResume) > 0) {
            Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount - beforeResume);
            for (int i = beforeResume + 1; i < expectedRecordCount; i++) {
                assertThat(dbChanges).contains(entry(i + 1, i));
            }
        }
    }

    @Test
    public void snapshotWithAdditionalCondition() throws Exception {
        // Testing.Print.enable();

        int expectedCount = 10, expectedValue = 12345678;
        populateTable();
        populateTableWithSpecificValue(2000, expectedCount, expectedValue);
        waitForCdcTransactionPropagation(3);
        final Configuration config = config().build();
        startAndConsumeTillEnd(connectorClass(), config);
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(Optional.of(String.format("\"aa = %s\"", expectedValue)), Optional.empty(),
                tableDataCollectionId());

        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(expectedCount,
                x -> true, null);
        assertEquals(expectedCount, dbChanges.size());
        assertTrue(dbChanges.values().stream().allMatch(v -> (((Struct) v.value()).getStruct("after")
                .getInt32(valueFieldName())).equals(expectedValue)));
    }

    @Test
    public void shouldExecuteRegularSnapshotWhenAdditionalConditionEmpty() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        final int recordsCount = ROW_COUNT;

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(Optional.of("\"\""), Optional.empty(), tableDataCollectionId());

        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(recordsCount,
                x -> true, null);
        assertEquals(recordsCount, dbChanges.size());
    }

    @Test
    public void snapshotWithAdditionalConditionWithRestart() throws Exception {
        // Testing.Print.enable();

        int expectedCount = 1000, expectedValue = 12345678;
        populateTable();
        populateTableWithSpecificValue(2000, expectedCount, expectedValue);
        waitForCdcTransactionPropagation(3);
        final Configuration config = config().build();
        startAndConsumeTillEnd(connectorClass(), config);
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(Optional.of(String.format("\"aa = %s\"", expectedValue)), Optional.empty(),
                tableDataCollectionId());

        final AtomicInteger recordCounter = new AtomicInteger();
        final AtomicBoolean restarted = new AtomicBoolean();
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedCount,
                x -> true, x -> {
                    if (recordCounter.addAndGet(x.size()) > 50 && !restarted.get()) {
                        stopConnector();
                        assertConnectorNotRunning();

                        start(connectorClass(), config);
                        waitForConnectorToStart();
                        restarted.set(true);
                    }
                });
        assertEquals(expectedCount, dbChanges.size());
        assertTrue(dbChanges.values().stream().allMatch(v -> v.equals(expectedValue)));
    }

    @Test
    public void snapshotWithSurrogateKey() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(Optional.empty(), Optional.of("\"aa\""), tableDataCollectionId());

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    public void snapshotWithAdditionalConditionWithSurrogateKey() throws Exception {
        // Testing.Print.enable();

        int expectedCount = 10, expectedValue = 12345678;
        populateTable();
        populateTableWithSpecificValue(2000, expectedCount, expectedValue);
        waitForCdcTransactionPropagation(3);
        final Configuration config = config().build();
        startAndConsumeTillEnd(connectorClass(), config);
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(Optional.of(String.format("\"aa = %s\"", expectedValue)), Optional.of("\"aa\""),
                tableDataCollectionId());

        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(expectedCount,
                x -> true, null);
        assertEquals(expectedCount, dbChanges.size());
        assertTrue(dbChanges.values().stream().allMatch(v -> (((Struct) v.value()).getStruct("after")
                .getInt32(valueFieldName())).equals(expectedValue)));
    }

    @Test
    public void testNotification() throws Exception {

        populateTable();
        startConnector(x -> x.with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink")
                .with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, defaultIncrementalSnapshotChunkSize())
                .with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification"), loggingCompletion(), false);

        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        sendAdHocSnapshotSignal();

        List<SourceRecord> records = new ArrayList<>();
        String topicName = topicName();

        List<SourceRecord> notifications = new ArrayList<>();
        consumeRecords(100, record -> {
            if (topicName.equalsIgnoreCase(record.topic())) {
                records.add(record);
            }
            if ("io.debezium.notification".equals(record.topic())) {
                notifications.add(record);
            }
        });

        sendPauseSignal();

        consumeAvailableRecords(record -> {
            if (topicName.equalsIgnoreCase(record.topic())) {
                records.add(record);
            }
            if ("io.debezium.notification".equals(record.topic())) {
                notifications.add(record);
            }
        });

        sendResumeSignal();

        SourceRecords sourceRecords = consumeRecordsByTopicUntil(incrementalSnapshotCompleted());

        records.addAll(sourceRecords.recordsForTopic(topicName()));
        notifications.addAll(sourceRecords.recordsForTopic("io.debezium.notification"));

        List<Integer> values = records.stream()
                .map(r -> ((Struct) r.value()))
                .map(getRecordValue())
                .collect(Collectors.toList());

        for (int i = 0; i < ROW_COUNT - 1; i++) {
            assertThat(values.get(i)).isEqualTo(i);
        }

        assertCorrectIncrementalSnapshotNotification(notifications);
    }

    protected int defaultIncrementalSnapshotChunkSize() {
        return 1;
    }

    private static BiPredicate<Integer, SourceRecord> incrementalSnapshotCompleted() {
        return (recordsConsumed, record) -> record.topic().equals("io.debezium.notification") &&
                ((Struct) record.value()).getString("aggregate_type").equals("Incremental Snapshot") &&
                ((Struct) record.value()).getString("type").equals("COMPLETED");
    }

    private void assertCorrectIncrementalSnapshotNotification(List<SourceRecord> notifications) {
        List<Struct> incrementalSnapshotNotification = notifications.stream().map(s -> ((Struct) s.value()))
                .filter(s -> s.getString("aggregate_type").equals("Incremental Snapshot"))
                .collect(Collectors.toList());

        assertThat(incrementalSnapshotNotification.stream().anyMatch(s -> s.getString("type").equals("STARTED"))).isTrue();
        assertThat(incrementalSnapshotNotification.stream().anyMatch(s -> s.getString("type").equals("PAUSED"))).isTrue();
        assertThat(incrementalSnapshotNotification.stream().anyMatch(s -> s.getString("type").equals("RESUMED"))).isTrue();
        assertThat(incrementalSnapshotNotification.stream().anyMatch(s -> s.getString("type").equals("IN_PROGRESS"))).isTrue();
        assertThat(incrementalSnapshotNotification.stream().anyMatch(s -> s.getString("type").equals("TABLE_SCAN_COMPLETED"))).isTrue();
        assertThat(incrementalSnapshotNotification.stream().anyMatch(s -> s.getString("type").equals("COMPLETED"))).isTrue();

        assertThat(incrementalSnapshotNotification.stream().map(s -> s.getString("id"))
                .distinct()
                .collect(Collectors.toList())).contains("ad-hoc");

        Struct inProgress = incrementalSnapshotNotification.stream().filter(s -> s.getString("type").equals("IN_PROGRESS")).findFirst().get();
        assertThat(inProgress.getMap("additional_data"))
                .containsEntry("current_collection_in_progress", tableDataCollectionId())
                .containsEntry("maximum_key", "1000")
                .containsEntry("last_processed_key", String.valueOf(defaultIncrementalSnapshotChunkSize()));

        Struct completed = incrementalSnapshotNotification.stream().filter(s -> s.getString("type").equals("TABLE_SCAN_COMPLETED")).findFirst().get();
        assertThat(completed.getMap("additional_data"))
                .containsEntry("total_rows_scanned", "1000");
    }

    private Function<Struct, Integer> getRecordValue() {
        return s -> s.getStruct("after").getInt32(valueFieldName());
    }

    protected void sendAdHocSnapshotSignalAndWait(String... collectionIds) throws Exception {
        // Sends the adhoc snapshot signal and waits for the signal event to have been received
        if (collectionIds.length == 0) {
            sendAdHocSnapshotSignal();
        }
        else {
            sendAdHocSnapshotSignal(collectionIds);
        }

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            final AtomicBoolean result = new AtomicBoolean(false);
            consumeAvailableRecords(r -> {
                if (r.topic().endsWith(signalTableNameSanitized())) {
                    result.set(true);
                }
            });
            return result.get();
        });
    }

    protected void sendAdHocSnapshotStopSignalAndWait(String... collectionIds) throws Exception {
        sendAdHocSnapshotStopSignal(collectionIds);

        // Wait for stop signal received and at least one incremental snapshot record
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            final AtomicBoolean stopSignal = new AtomicBoolean(false);
            consumeAvailableRecords(r -> {
                if (r.topic().endsWith(signalTableNameSanitized())) {
                    final Struct after = ((Struct) r.value()).getStruct(Envelope.FieldName.AFTER);
                    if (after != null) {
                        final String type = after.getString(getSignalTypeFieldName());
                        if (StopSnapshot.NAME.equals(type)) {
                            stopSignal.set(true);
                        }
                    }
                }
            });
            return stopSignal.get();
        });
    }

    protected boolean consumeAnyRemainingIncrementalSnapshotEventsAndCheckForStopMessage(LogInterceptor interceptor, String stopMessage) throws Exception {
        // When an incremental snapshot is stopped, there may be some residual open/close events that
        // have been written concurrently to the signal table after the stop signal. We want to make
        // sure that those have all been read before stopping the connector.
        final AtomicBoolean stopMessageFound = new AtomicBoolean(false);
        Awaitility.await().atMost(60, TimeUnit.SECONDS)
                .pollDelay(5, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> {
                    if (interceptor.containsMessage(stopMessage)) {
                        stopMessageFound.set(true);
                    }
                    return consumeAvailableRecords(r -> {
                    }) == 0;
                });
        return stopMessageFound.get();
    }

    @Override
    protected int getMaximumEnqueuedRecordCount() {
        return ROW_COUNT * 3;
    }
}
