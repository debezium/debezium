/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.Flaky;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.kafka.KafkaClusterUtils;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.signal.channels.FileSignalChannel;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.strimzi.test.container.StrimziKafkaCluster;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;

public abstract class BinlogReadOnlyIncrementalSnapshotIT<C extends SourceConnector> extends BinlogIncrementalSnapshotIT<C> {

    public static final String EXCLUDED_TABLE = "b";

    private static final int PARTITION_NO = 0;

    private static final String NOTIFICATION_TOPIC = "io.debezium.notification";

    private SnapshotStopSynchronization snapshotStopSynchronization;

    @BeforeEach
    void before() throws Exception {
        super.before();
        KafkaClusterUtils.createTopic(getSignalsTopic(), 1, (short) 1, kafkaCluster.getBootstrapServers());
    }

    @BeforeAll
    static void startKafka() {
        Map<String, String> props = new HashMap<>();
        props.put("auto.create.topics.enable", "false");

        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withAdditionalKafkaConfiguration(props)
                .withSharedNetwork()
                .build();
        kafkaCluster.start();
    }

    @AfterAll
    static void stopKafka() {
        if (kafkaCluster != null) {
            kafkaCluster.stop();
        }
    }

    protected Configuration.Builder config() {
        final var builder = super.config()
                .with(BinlogConnectorConfig.TABLE_EXCLUDE_LIST, DATABASE.getDatabaseName() + "." + EXCLUDED_TABLE)
                .with(BinlogConnectorConfig.READ_ONLY_CONNECTION, true)
                .with(KafkaSignalChannel.SIGNAL_TOPIC, getSignalsTopic())
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers())
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source,kafka")
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, String.format("%s:%s", DATABASE.qualifiedTableName("a42"), "pk1,pk2,pk3,pk4"));
        if (snapshotStopSynchronization != null) {
            builder.with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink")
                    .with(SinkNotificationChannel.NOTIFICATION_TOPIC, NOTIFICATION_TOPIC);
        }
        return builder;
    }

    @Override
    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        if (snapshotStopSynchronization == null) {
            super.startConnector(custConfig);
            return;
        }
        // Sink notifications also report the initial schema-only snapshot; only data records are unexpected here.
        super.startConnector(custConfig, loggingCompletion(), false);
        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());
        consumeAvailableRecords(record -> assertThat(record.topic()).isEqualTo(NOTIFICATION_TOPIC));
    }

    @Override
    protected void removeCapturedCollectionFromInProgressIncrementalSnapshot(int collectionIndexToRemove) throws Exception {
        try (var synchronization = new SnapshotStopSynchronization(tableDataCollectionIds().get(0), getWaitDurationInSeconds())) {
            snapshotStopSynchronization = synchronization;
            super.removeCapturedCollectionFromInProgressIncrementalSnapshot(collectionIndexToRemove);

            // Completion follows the snapshot READ records, including those from the removed collection.
            Awaitility.await().atMost(getWaitDurationInSeconds()).until(() -> {
                consumeAvailableRecords(synchronization::observeRecord);
                return synchronization.snapshotCompleted;
            });
            assertThat(synchronization.failure).as("First chunk and stop commit synchronization").isNull();

            final String retainedTopic = topicNames().get(1 - collectionIndexToRemove);
            // Concurrent INSERTs may also be captured if this collection's maximum key is read after they commit.
            assertThat(synchronization.snapshotReads.getOrDefault(retainedTopic, 0))
                    .as("Snapshot READ records for the retained collection").isGreaterThanOrEqualTo(ROW_COUNT);
            final int removedReads = synchronization.snapshotReads.getOrDefault(topicNames().get(collectionIndexToRemove), 0);
            if (collectionIndexToRemove == 0) {
                assertThat(removedReads).as("Snapshot READ records for the partially captured collection").isBetween(1, ROW_COUNT - 1);
            }
            else {
                assertThat(removedReads).as("Snapshot READ records for the not yet captured collection").isZero();
            }
        }
        finally {
            snapshotStopSynchronization = null;
        }
    }

    @Override
    protected void sendAdHocSnapshotStopSignal(String... dataCollectionIds) throws SQLException {
        if (snapshotStopSynchronization == null) {
            super.sendAdHocSnapshotStopSignal(dataCollectionIds);
            return;
        }
        try {
            assertThat(snapshotStopSynchronization.firstChunkStarted.await(getWaitDurationInSeconds().toMillis(), TimeUnit.MILLISECONDS))
                    .as("First incremental snapshot chunk started").isTrue();
            super.sendAdHocSnapshotStopSignal(dataCollectionIds);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting for the first incremental snapshot chunk", e);
        }
        finally {
            snapshotStopSynchronization.stopCommitted.countDown();
        }
    }

    @Override
    protected SourceRecords consumeRecordsByTopic(int numRecords, boolean assertRecords) throws InterruptedException {
        final SourceRecords records = super.consumeRecordsByTopic(numRecords, assertRecords);
        if (snapshotStopSynchronization != null) {
            records.allRecordsInOrder().forEach(snapshotStopSynchronization::observeRecord);
        }
        return records;
    }

    private static class SnapshotStopSynchronization extends LogInterceptor implements AutoCloseable {
        private final Logger snapshotLogger = (Logger) LoggerFactory.getLogger(AbstractIncrementalSnapshotChangeEventSource.class);
        private final Level previousLevel;
        private final String firstChunkMessage;
        private final Duration timeout;
        private final CountDownLatch firstChunkStarted = new CountDownLatch(1);
        private final CountDownLatch stopCommitted = new CountDownLatch(1);
        private final AtomicBoolean firstChunk = new AtomicBoolean(true);
        private final Map<String, Integer> snapshotReads = new HashMap<>();
        private volatile Throwable failure;
        private boolean snapshotCompleted;

        private SnapshotStopSynchronization(String firstCollection, Duration timeout) {
            super(AbstractIncrementalSnapshotChangeEventSource.class);
            firstChunkMessage = "Incremental snapshot for table '" + firstCollection + "' will end at position ";
            this.timeout = timeout;
            previousLevel = snapshotLogger.getLevel();
            snapshotLogger.setLevel(Level.INFO);
        }

        @Override
        protected void append(ILoggingEvent event) {
            if (event.getFormattedMessage().startsWith(firstChunkMessage) && firstChunk.compareAndSet(true, false)) {
                // Commit the stop between the first chunk's low and high watermarks. With unchanged GTID
                // watermarks, read-only snapshots can finish before the stop transaction commits.
                // Wait only for the commit, not signal processing, which must run on this same connector thread.
                firstChunkStarted.countDown();
                try {
                    if (!stopCommitted.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                        failure = new IllegalStateException("Stop signal did not commit before the first chunk could continue");
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failure = e;
                }
            }
        }

        private void observeRecord(SourceRecord record) {
            if (record.value() instanceof Struct value) {
                if (NOTIFICATION_TOPIC.equals(record.topic())) {
                    if ("Incremental Snapshot".equals(value.getString("aggregate_type")) && "COMPLETED".equals(value.getString("type"))) {
                        snapshotCompleted = true;
                    }
                }
                else if (value.schema().field(Envelope.FieldName.OPERATION) != null
                        && Envelope.Operation.READ.code().equals(value.getString(Envelope.FieldName.OPERATION))) {
                    snapshotReads.merge(record.topic(), 1, Integer::sum);
                }
            }
        }

        @Override
        public void close() {
            stopCommitted.countDown();
            snapshotLogger.detachAppender(this);
            stop();
            snapshotLogger.setLevel(previousLevel);
        }
    }

    protected String getSignalsTopic() {
        return DATABASE.getDatabaseName() + "signals_topic";
    }

    protected void sendExecuteSnapshotKafkaSignal() throws ExecutionException, InterruptedException {
        sendExecuteSnapshotKafkaSignal(tableDataCollectionId());
    }

    protected void sendExecuteSnapshotKafkaSignal(String fullTableNames) throws ExecutionException, InterruptedException {
        String signalValue = String.format(
                "{\"type\":\"execute-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}}",
                fullTableNames);
        sendKafkaSignal(signalValue);
    }

    protected void sendStopSnapshotKafkaSignal() throws ExecutionException, InterruptedException {
        sendStopSnapshotKafkaSignal(tableDataCollectionId());
    }

    protected void sendStopSnapshotKafkaSignal(String fullTableNames) throws ExecutionException, InterruptedException {
        String signalValue = String.format(
                "{\"type\":\"stop-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}}",
                fullTableNames);
        sendKafkaSignal(signalValue);
    }

    protected void sendPauseSnapshotKafkaSignal() throws ExecutionException, InterruptedException {
        sendKafkaSignal("{\"type\":\"pause-snapshot\",\"data\": {\"type\": \"INCREMENTAL\"}}");
    }

    protected void sendResumeSnapshotKafkaSignal() throws ExecutionException, InterruptedException {
        sendKafkaSignal("{\"type\":\"resume-snapshot\",\"data\": {\"type\": \"INCREMENTAL\"}}");
    }

    protected void sendKafkaSignal(String signalValue) throws ExecutionException, InterruptedException {
        final ProducerRecord<String, String> executeSnapshotSignal = new ProducerRecord<>(getSignalsTopic(), PARTITION_NO, SERVER_NAME, signalValue);

        final Configuration signalProducerConfig = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "signals")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(signalProducerConfig.asProperties())) {
            producer.send(executeSnapshotSignal).get();
        }
    }

    @Test
    void emptyHighWatermark() throws Exception {
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
    void filteredEvents() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        sendExecuteSnapshotKafkaSignal();

        Thread t = new Thread(() -> {
            try (JdbcConnection connection = databaseConnection()) {
                connection.setAutoCommit(false);
                for (int i = 0; !Thread.interrupted(); i++) {
                    connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk, aa) VALUES (%s, %s)",
                            EXCLUDED_TABLE,
                            i + ROW_COUNT + 1,
                            i + ROW_COUNT));
                    connection.commit();
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        t.setDaemon(true);
        t.setName("filtered-binlog-events-thread");
        try {
            t.start();
            final int expectedRecordCount = ROW_COUNT;
            final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
            for (int i = 0; i < expectedRecordCount; i++) {
                assertThat(dbChanges).contains(entry(i + 1, i));
            }
        }
        finally {
            t.interrupt();
        }
    }

    @Test
    void inserts4Pks() throws Exception {
        // Testing.Print.enable();

        populate4PkTable();
        startConnector();

        sendExecuteSnapshotKafkaSignal(DATABASE.qualifiedTableName("a4"));

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32("pk1") * 1_000 + k.getInt32("pk2") * 100 + k.getInt32("pk3") * 10 + k.getInt32("pk4"),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                DATABASE.topicForTable("a4"),
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    void inserts4PksWithSignalFile() throws Exception {
        // Testing.Print.enable();

        populate4PkTable();
        startConnector(c -> c.with(FileSignalChannel.SIGNAL_FILE, signalsFile.toString())
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "file"));

        sendExecuteSnapshotFileSignal(DATABASE.qualifiedTableName("a4"));

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32("pk1") * 1_000 + k.getInt32("pk2") * 100 + k.getInt32("pk3") * 10 + k.getInt32("pk4"),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                DATABASE.topicForTable("a4"),
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @FixFor("DBZ-7441")
    @Test
    void aSignalAddedToFileWhenConnectorIsStoppedShouldBeProcessedWhenItStarts() throws Exception {
        // Testing.Print.enable();

        populate4PkTable();
        sendExecuteSnapshotFileSignal(DATABASE.qualifiedTableName("a4"));

        startConnector(c -> c.with(FileSignalChannel.SIGNAL_FILE, signalsFile.toString())
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "file"), loggingCompletion(), false);

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32("pk1") * 1_000 + k.getInt32("pk2") * 100 + k.getInt32("pk3") * 10 + k.getInt32("pk4"),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                DATABASE.topicForTable("a4"),
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    @FixFor("DBZ-5453")
    @Flaky("DBZ-7572")
    public void testStopSnapshotKafkaSignal() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(AbstractIncrementalSnapshotChangeEventSource.class);

        populateTable();
        startConnector(x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 1));
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        assertNoRecordsToConsume();

        sendExecuteSnapshotKafkaSignal();

        consumeMixedWithIncrementalSnapshot(1);

        sendStopSnapshotKafkaSignal();

        final List<SourceRecord> records = new ArrayList<>();
        final String topicName = topicName();
        final String tableRemoveMessage = String.format("Removed '%s' from incremental snapshot collection list.", tableDataCollectionId());

        Awaitility.await()
                .atMost(Duration.ofMinutes(2))
                .until(() -> {
                    consumeAvailableRecords(record -> {
                        if (topicName.equalsIgnoreCase(record.topic())) {
                            records.add(record);
                        }
                    });

                    return logInterceptor.containsMessage(tableRemoveMessage);
                });

    }

    @Test
    void testPauseDuringSnapshotKafkaSignal() throws Exception {
        populateTable();
        startConnector(x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 1));
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendExecuteSnapshotKafkaSignal();

        List<SourceRecord> records = new ArrayList<>();
        String topicName = topicName();
        Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(100);

        sendPauseSnapshotKafkaSignal();

        consumeAvailableRecords(record -> {
            if (topicName.equalsIgnoreCase(record.topic())) {
                records.add(record);
            }
        });
        int beforeResume = records.size() + dbChanges.size();

        sendResumeSnapshotKafkaSignal();

        dbChanges = consumeMixedWithIncrementalSnapshot(ROW_COUNT - beforeResume);
        for (int i = beforeResume + 1; i < ROW_COUNT; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
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
}
