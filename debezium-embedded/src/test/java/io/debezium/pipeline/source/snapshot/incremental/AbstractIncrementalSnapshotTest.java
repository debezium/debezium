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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
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
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenConnectorUnderTest;
import io.debezium.junit.SkipWhenConnectorUnderTest.Connector;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.kafka.KafkaCluster;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.signal.actions.snapshotting.StopSnapshot;

public abstract class AbstractIncrementalSnapshotTest<T extends SourceConnector> extends AbstractSnapshotTest<T> {

    protected static KafkaCluster kafka;

    protected String getSignalTypeFieldName() {
        return "type";
    }

    protected abstract String noPKTopicName();

    protected abstract String noPKTableName();

    protected String noPKTableDataCollectionId() {
        return noPKTableName();
    }

    protected String returnedIdentifierName(String queriedID) {
        return queriedID;
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
    public void insertsWithoutPks() throws Exception {
        // Testing.Print.enable();

        try (JdbcConnection connection = databaseConnection()) {
            populate4PkTable(connection, noPKTableName());
        }

        startConnector();

        sendAdHocSnapshotSignal(noPKTableDataCollectionId());

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32(returnedIdentifierName("pk1")) * 1_000 + k.getInt32(returnedIdentifierName("pk2")) * 100
                        + k.getInt32(returnedIdentifierName("pk3")) * 10 + k.getInt32(returnedIdentifierName("pk4")),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                noPKTopicName(),
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    public void insertsWithoutPksAndNull() throws Exception {
        // Testing.Print.enable();

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int pk1 = 10; pk1 <= 30; pk1 += 10) {
                String pk1Str = pk1 == 10 ? "NULL" : String.valueOf(pk1);
                for (int pk2 = 1; pk2 <= 3; pk2++) {
                    String pk2Str = pk2 == 1 ? "NULL" : String.valueOf(pk2);
                    int pkSum = pk1 + pk2;
                    connection.executeWithoutCommitting(String.format(
                            "INSERT INTO %s (pk1, pk2, pk3, pk4, aa) VALUES (%s, %s, 0, 0, %s)",
                            noPKTableName(), pk1Str, pk2Str, pkSum));
                }
            }
            connection.commit();
        }

        // Go only one row at a time so that each possible window boundary with a NULL is tested: this is important for this test
        startConnector(cfg -> cfg.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 1));

        sendAdHocSnapshotSignal(noPKTableDataCollectionId());

        final int expectedRecordCount = 9;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> Objects.requireNonNullElse(k.getInt32(returnedIdentifierName("pk1")), 10)
                        + Objects.requireNonNullElse(k.getInt32(returnedIdentifierName("pk2")), 1),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                noPKTopicName(),
                null);
        for (int pk1 = 10; pk1 <= 30; pk1 += 10) {
            for (int pk2 = 1; pk2 <= 3; pk2++) {
                assertThat(dbChanges).contains(entry(pk1 + pk2, pk1 + pk2));
            }
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
    @FixFor("DBZ-6945")
    public void snapshotWithDuplicateDataCollections() throws Exception {
        populateTable();
        startConnector();
        sendAdHocSnapshotSignal(tableDataCollectionId(), tableDataCollectionId());

        final int expectedRecordCount = ROW_COUNT;
        Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }

        SourceRecords sourceRecords = consumeRecordsByTopic(1, 1);
        assertTrue(Objects.isNull(sourceRecords.recordsForTopic(topicName())));

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

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(String.format("\"aa = %s\"", expectedValue), "",
                tableDataCollectionId());

        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(expectedCount,
                x -> true, null);
        assertEquals(expectedCount, dbChanges.size());
        assertTrue(dbChanges.values().stream().allMatch(v -> (((Struct) v.value()).getStruct("after")
                .getInt32(valueFieldName())).equals(expectedValue)));
    }

    @Test
    public void snapshotWithNewAdditionalConditionsField() throws Exception {
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

        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(Map.of(tableDataCollectionId(), String.format("aa = %s", expectedValue)), "",
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

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey("\"\"", "", tableDataCollectionId());

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

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(String.format("\"aa = %s\"", expectedValue), "", tableDataCollectionId());

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

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey("", "\"aa\"", tableDataCollectionId());

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

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(String.format("\"aa = %s\"", expectedValue), "\"aa\"", tableDataCollectionId());

        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(expectedCount,
                x -> true, null);
        assertEquals(expectedCount, dbChanges.size());
        assertTrue(dbChanges.values().stream().allMatch(v -> (((Struct) v.value()).getStruct("after")
                .getInt32(valueFieldName())).equals(expectedValue)));
    }

    @Test
    // TODO seems slow try to speedup
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

    @Test
    public void insertInsertWatermarkingStrategy() throws Exception {
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

        assertOpenCloseEventCount(rs -> {
            rs.next();
            assertThat(rs.getInt(1)).isNotZero();
        });
    }

    @Test
    public void insertDeleteWatermarkingStrategy() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector(x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_WATERMARKING_STRATEGY, "insert_delete")
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)); // Remove tombstone to avoid failure of VerifyRecord.isValid

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

        assertOpenCloseEventCount(rs -> {
            rs.next();
            assertThat(rs.getInt(1)).isZero();
        });
    }

    private void assertOpenCloseEventCount(JdbcConnection.ResultSetConsumer consumer) throws SQLException {
        try (JdbcConnection connection = databaseConnection()) {
            connection.query("SELECT count(id) from " + signalTableName() + " where type='snapshot-window-close'", consumer);
        }
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

}
