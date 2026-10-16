/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.signal.actions.AbstractSnapshotSignal;
import io.debezium.pipeline.signal.actions.snapshotting.ExecuteSnapshot;
import io.debezium.pipeline.signal.actions.snapshotting.StopSnapshot;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Strings;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 13, minor = 0, reason = "Function pg_current_snapshot() not supported until PostgreSQL 13")
public class ReadOnlyIncrementalSnapshotIT extends IncrementalSnapshotIT {

    private final LogInterceptor executeSignalInterceptor = new LogInterceptor(ExecuteSnapshot.class);
    private final LogInterceptor stopSignalInterceptor = new LogInterceptor(StopSnapshot.class);

    @Test
    @Override
    @FixFor("debezium/dbz#2806")
    public void testNotification() throws Exception {
        final List<SourceRecord> snapshotRecords = new ArrayList<>();
        final List<SourceRecord> notifications = new ArrayList<>();
        final Consumer<SourceRecord> collect = record -> {
            if ("io.debezium.notification".equals(record.topic())) {
                notifications.add(record);
            }
            else if (topicName().equals(record.topic()) && record.value() instanceof Struct value && "r".equals(value.getString("op"))) {
                snapshotRecords.add(record);
            }
        };

        try (var synchronization = new NotificationSynchronization(getWaitDurationInSeconds())) {
            startConnector(x -> x.with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink")
                    .with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, defaultIncrementalSnapshotChunkSize())
                    .with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification"), loggingCompletion(), false);
            awaitNotificationCheckpoint("streaming ready", synchronization, () -> synchronization.streamingReady.getCount() == 0, collect);

            // Insert after the replication stream is established, but before allowing it to read.
            // This preserves the insert backlog from the reported failure without racing the first chunk.
            populateTable();
            sendAdHocSnapshotSignal();
            awaitNotificationCheckpoint("first chunk loaded", synchronization, () -> synchronization.firstChunkLoaded.getCount() == 0, collect);
            synchronization.allowStreaming.countDown();

            // The first window must be emitted and cleared before pause/resume: resume reads the next chunk.
            awaitNotificationCheckpoint("streaming between chunks", synchronization, () -> synchronization.betweenChunks.getCount() == 0, collect);
            sendPauseSignal();
            awaitNotificationCheckpoint("PAUSED notification", synchronization, () -> hasIncrementalNotification(notifications, "PAUSED"), collect);
            assertThat(hasIncrementalNotification(notifications, "COMPLETED")).isFalse();

            // Let the pending readChunk observe the paused state and take its early-return branch.
            // Hold it there until resume has finished loading its chunk on the signal thread.
            synchronization.allowPausedRead.countDown();
            awaitNotificationCheckpoint("paused chunk read", synchronization, () -> synchronization.pausedRead.getCount() == 0, collect);
            sendResumeSignal();
            awaitNotificationCheckpoint("resume chunk loaded", synchronization,
                    () -> synchronization.resumedChunkLoaded.getCount() == 0 && hasIncrementalNotification(notifications, "RESUMED"), collect);
            synchronization.allowAfterResume.countDown();

            awaitNotificationCheckpoint("snapshot completed", synchronization, () -> hasIncrementalNotification(notifications, "COMPLETED"), collect);
            final List<Integer> keys = snapshotRecords.stream()
                    .map(record -> ((Struct) record.key()).getInt32(pkFieldName()))
                    .collect(Collectors.toList());
            assertThat(keys).containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(1, ROW_COUNT).boxed().collect(Collectors.toList()));
            assertThat(snapshotRecords).allSatisfy(record -> assertThat(getRecordValue().apply((Struct) record.value()))
                    .isEqualTo(((Struct) record.key()).getInt32(pkFieldName()) - 1));
            assertCorrectIncrementalSnapshotNotification(notifications);
        }
        finally {
            stopConnector();
        }
    }

    private void awaitNotificationCheckpoint(String description, NotificationSynchronization synchronization, BooleanSupplier reached,
                                             Consumer<SourceRecord> collect) {
        Awaitility.await(description).atMost(getWaitDurationInSeconds()).pollInterval(Duration.ofMillis(10)).until(() -> {
            consumeAvailableRecords(collect);
            assertThat(synchronization.failure.get()).as("Snapshot notification synchronization").isNull();
            return reached.getAsBoolean();
        });
    }

    private static boolean hasIncrementalNotification(List<SourceRecord> notifications, String type) {
        return notifications.stream().map(record -> (Struct) record.value())
                .anyMatch(value -> "Incremental Snapshot".equals(value.getString("aggregate_type")) && type.equals(value.getString("type")));
    }

    /**
     * Coordinates real Kafka signals and WAL processing using log-triggered latches,
     * An unsynchronized appender is needed:
     * holding LogInterceptor's appender monitor would also block the signal thread.
     */
    private static class NotificationSynchronization extends UnsynchronizedAppenderBase<ILoggingEvent> implements AutoCloseable {
        private final Logger streamingLogger = (Logger) LoggerFactory.getLogger(PostgresStreamingChangeEventSource.class);
        private final Logger snapshotLogger = (Logger) LoggerFactory.getLogger(AbstractIncrementalSnapshotChangeEventSource.class);
        private final Level previousStreamingLevel = streamingLogger.getLevel();
        private final Level previousSnapshotLevel = snapshotLogger.getLevel();
        private final Duration timeout;
        private final CountDownLatch streamingReady = new CountDownLatch(1);
        private final CountDownLatch firstChunkLoaded = new CountDownLatch(1);
        private final CountDownLatch betweenChunks = new CountDownLatch(1);
        private final CountDownLatch pausedRead = new CountDownLatch(1);
        private final CountDownLatch resumedChunkLoaded = new CountDownLatch(1);
        private final CountDownLatch allowStreaming = new CountDownLatch(1);
        private final CountDownLatch allowPausedRead = new CountDownLatch(1);
        private final CountDownLatch allowAfterResume = new CountDownLatch(1);
        private final AtomicBoolean firstStreamingChunk = new AtomicBoolean(true);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private volatile Thread streamingThread;

        private NotificationSynchronization(Duration timeout) {
            this.timeout = timeout;
            start();
            streamingLogger.setLevel(Level.INFO);
            snapshotLogger.setLevel(Level.TRACE);
            streamingLogger.addAppender(this);
            snapshotLogger.addAppender(this);
        }

        @Override
        protected void append(ILoggingEvent event) {
            if (streamingLogger.getName().equals(event.getLoggerName()) && "Processing messages".equals(event.getMessage())) {
                streamingThread = Thread.currentThread();
                streamingReady.countDown();
                awaitGate(allowStreaming, "first chunk before streaming");
            }
            else if (snapshotLogger.getName().equals(event.getLoggerName())) {
                if ("Window close emitted".equals(event.getMessage()) && Thread.currentThread() != streamingThread) {
                    if (firstChunkLoaded.getCount() != 0) {
                        firstChunkLoaded.countDown();
                    }
                    else {
                        resumedChunkLoaded.countDown();
                    }
                }
                else if ("Reading chunk".equals(event.getMessage()) && Thread.currentThread() == streamingThread
                        && firstStreamingChunk.compareAndSet(true, false)) {
                    betweenChunks.countDown();
                    awaitGate(allowPausedRead, "pause notification before next chunk");
                }
                else if ("Incremental snapshot was paused.".equals(event.getMessage()) && Thread.currentThread() == streamingThread) {
                    pausedRead.countDown();
                    awaitGate(allowAfterResume, "resume chunk before streaming continues");
                }
            }
        }

        private void awaitGate(CountDownLatch gate, String description) {
            try {
                if (!gate.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    failure.compareAndSet(null, new IllegalStateException("Timed out waiting for " + description));
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failure.compareAndSet(null, e);
            }
        }

        @Override
        public void close() {
            streamingLogger.detachAppender(this);
            snapshotLogger.detachAppender(this);
            allowStreaming.countDown();
            allowPausedRead.countDown();
            allowAfterResume.countDown();
            stop();
            streamingLogger.setLevel(previousStreamingLevel);
            snapshotLogger.setLevel(previousSnapshotLevel);
        }
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "kafka")
                .with(PostgresConnectorConfig.READ_ONLY_CONNECTION, true)
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers())
                .with(KafkaSignalChannel.SIGNAL_TOPIC, getSignalsTopic())
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "s1.a42:pk1,pk2,pk3,pk4")
                // DBZ-4272 required to allow dropping columns just before an incremental snapshot
                .with("database.autosave", "conservative");
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        final String tableIncludeList;
        if (signalTableOnly) {
            tableIncludeList = "s1.b";
        }
        else {
            tableIncludeList = "s1.a,s1.b";
        }
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(PostgresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "kafka")
                .with(PostgresConnectorConfig.READ_ONLY_CONNECTION, true)
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers())
                .with(KafkaSignalChannel.SIGNAL_TOPIC, getSignalsTopic())
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "s1.a42:pk1,pk2,pk3,pk4")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                // DBZ-4272 required to allow dropping columns just before an incremental snapshot
                .with("database.autosave", "conservative");
    }

    @Override
    protected void sendAdHocSnapshotSignal(String... dataCollectionIds) {

        String signalValue = String.format(
                "{\"id\":\"ad-hoc\", \"type\":\"execute-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}}",
                String.join("\",\"", dataCollectionIds));
        try {
            sendKafkaSignal(signalValue);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send signal", e);
        }
    }

    @Override
    protected void sendAdHocSnapshotStopSignal(String... dataCollectionIds) {

        String signalValue = "{\"type\":\"stop-snapshot\",\"data\": {\"type\": \"INCREMENTAL\"}}";

        if (dataCollectionIds.length > 0) {

            signalValue = String.format(
                    "{\"type\":\"stop-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}}",
                    String.join(",", dataCollectionIds));
        }

        try {
            sendKafkaSignal(signalValue);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send signal", e);
        }
    }

    @Override
    protected void sendPauseSignal() {
        try {
            sendKafkaSignal("{\"type\":\"pause-snapshot\",\"data\": {\"type\": \"INCREMENTAL\"}}");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send resume signal", e);
        }
    }

    @Override
    protected void sendResumeSignal() {
        try {
            sendKafkaSignal("{\"type\":\"resume-snapshot\",\"data\": {\"type\": \"INCREMENTAL\"}}");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send pause signal", e);
        }
    }

    @Override
    protected void sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(String additionalCondition, String surrogateKey,
                                                                                  AbstractSnapshotSignal.SnapshotType snapshotType,
                                                                                  String... dataCollectionIds) {
        final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                .map(x -> '"' + x + '"')
                .collect(Collectors.joining(", "));

        String signalValue;
        if (!Strings.isNullOrEmpty(additionalCondition) && !Strings.isNullOrEmpty(surrogateKey)) {
            signalValue = String.format(
                    "{\"type\":\"execute-snapshot\",\"data\": {\"type\": \"%s\",\"data-collections\": [%s], \"additional-conditions\": [%s], \"surrogate-key\": %s}}",
                    snapshotType.toString(), dataCollectionIdsList, additionalCondition, surrogateKey);
        }
        else if (!Strings.isNullOrEmpty(additionalCondition)) {
            signalValue = String.format(
                    "{\"type\":\"execute-snapshot\",\"data\": {\"type\": \"%s\",\"data-collections\": [%s], \"additional-conditions\": [%s]}}",
                    snapshotType.toString(), dataCollectionIdsList, additionalCondition);
        }
        else if (!Strings.isNullOrEmpty(surrogateKey)) {
            signalValue = String.format(
                    "{\"type\":\"execute-snapshot\",\"data\": {\"type\": \"%s\",\"data-collections\": [%s], \"surrogate-key\": %s}} `",
                    snapshotType.toString(), dataCollectionIdsList, surrogateKey);
        }
        else {
            signalValue = String.format(
                    "{\"type\":\"execute-snapshot\",\"data\": {\"type\": \"%s\",\"data-collections\": [%s]}}",
                    snapshotType.toString(), dataCollectionIdsList);
        }

        try {
            logger.info("Sending signal with message {}", signalValue);
            sendKafkaSignal(signalValue);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send signal", e);
        }
    }

    @Override
    protected void sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(Map<String, String> additionalConditions, String surrogateKey,
                                                                                   AbstractSnapshotSignal.SnapshotType snapshotType,
                                                                                   String... dataCollectionIds) {

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(buildAdditionalConditions(additionalConditions), surrogateKey, snapshotType, dataCollectionIds);
    }

    @Override
    protected Callable<Boolean> executeSignalWaiter() {
        return () -> executeSignalInterceptor.containsMessage("Requested 'INCREMENTAL' snapshot of data collections");
    }

    @Override
    protected Callable<Boolean> stopSignalWaiter() {
        return () -> stopSignalInterceptor.containsMessage("Requested stop of snapshot");
    }

    @Test
    @Override
    public void insertInsertWatermarkingStrategy() throws Exception {
        // test has not to be executed on read only
    }

    @Test
    @Override
    public void insertDeleteWatermarkingStrategy() throws Exception {
        // test has not to be executed on read only
    }

    @Test
    @Override
    public void snapshotPartitionedTable() throws Exception {
        super.snapshotPartitionedTable();
    }

    @Test
    @FixFor("DBZ-9016")
    public void processingOfMessagesShouldHappenOnlyWhenSnapshotIsRunning() throws Exception {
        // Testing.Print.enable();

        LogInterceptor logInterceptor = new LogInterceptor(AbstractIncrementalSnapshotChangeEventSource.class);
        populate4PkTable();
        startConnector();

        sendAdHocSnapshotSignal("s1.a4");

        Thread.sleep(5000);
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                final int id = i + ROW_COUNT + 1;
                final int pk1 = id / 1000;
                final int pk2 = (id / 100) % 10;
                final int pk3 = (id / 10) % 10;
                final int pk4 = id % 10;
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk1, pk2, pk3, pk4, aa) VALUES (%s, %s, %s, %s, %s)",
                        "s1.a4",
                        pk1,
                        pk2,
                        pk3,
                        pk4,
                        i + ROW_COUNT));
            }
            connection.commit();
        }

        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32("pk1") * 1_000 + k.getInt32("pk2") * 100 + k.getInt32("pk3") * 10 + k.getInt32("pk4"),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                "test_server.s1.a4",
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(true);
            for (int i = ROW_COUNT; i < ROW_COUNT * 2; i++) {
                final int id = i + ROW_COUNT + 1;
                final int pk1 = id / 1000;
                final int pk2 = (id / 100) % 10;
                final int pk3 = (id / 10) % 10;
                final int pk4 = id % 10;
                connection.execute(String.format("INSERT INTO %s (pk1, pk2, pk3, pk4, aa) VALUES (%s, %s, %s, %s, %s)",
                        "s1.a4",
                        pk1,
                        pk2,
                        pk3,
                        pk4,
                        i + ROW_COUNT));
            }
        }

        for (int i = 0; i < ROW_COUNT; i++) {
            assertThat(dbChanges).contains(entry(i + ROW_COUNT + 1, i + ROW_COUNT));
        }

        assertThat(logInterceptor.countOccurrences("Skipping read chunk because snapshot is not running")).isEqualTo(0L);

    }

    @Override
    protected void assertExpectedRecordsEnumPk(List<String> enumValues) throws InterruptedException {

        waitForAvailableRecords(5000, TimeUnit.SECONDS);

        final var records = consumeRecordsByTopic(enumValues.size()).allRecordsInOrder();
        for (int i = 0; i < enumValues.size(); i++) {
            var record = records.get(i);
            assertThat(((Struct) record.key()).getString("pk")).isEqualTo(enumValues.get(i));
            assertThat(((Struct) record.value()).getStruct("after").getInt32("aa")).isEqualTo(i);
        }
    }

    @Override
    protected Function<Configuration.Builder, Configuration.Builder> additionalConfiguration() {
        return x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 1)
                .with(Heartbeat.HEARTBEAT_INTERVAL_PROPERTY_NAME, 5000);
    }
}
