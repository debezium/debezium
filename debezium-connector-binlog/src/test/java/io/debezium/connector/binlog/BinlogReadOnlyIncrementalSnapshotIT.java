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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.ConditionalFail;
import io.debezium.junit.Flaky;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.kafka.KafkaClusterUtils;
import io.debezium.pipeline.signal.channels.FileSignalChannel;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.strimzi.test.container.StrimziKafkaCluster;

public abstract class BinlogReadOnlyIncrementalSnapshotIT<C extends SourceConnector> extends BinlogIncrementalSnapshotIT<C> {

    public static final String EXCLUDED_TABLE = "b";

    private static final int PARTITION_NO = 0;

    @Rule
    public ConditionalFail conditionalFail = new ConditionalFail();

    @Before
    public void before() throws Exception {
        super.before();
        KafkaClusterUtils.createTopic(getSignalsTopic(), 1, (short) 1, kafkaCluster.getBootstrapServers());
    }

    @BeforeClass
    public static void startKafka() {
        Map<String, String> props = new HashMap<>();
        props.put("auto.create.topics.enable", "false");

        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withAdditionalKafkaConfiguration(props)
                .withSharedNetwork()
                .build();
        kafkaCluster.start();
    }

    @AfterClass
    public static void stopKafka() {
        if (kafkaCluster != null) {
            kafkaCluster.stop();
        }
    }

    protected Configuration.Builder config() {
        return super.config()
                .with(BinlogConnectorConfig.TABLE_EXCLUDE_LIST, DATABASE.getDatabaseName() + "." + EXCLUDED_TABLE)
                .with(BinlogConnectorConfig.READ_ONLY_CONNECTION, true)
                .with(KafkaSignalChannel.SIGNAL_TOPIC, getSignalsTopic())
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers())
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source,kafka")
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, String.format("%s:%s", DATABASE.qualifiedTableName("a42"), "pk1,pk2,pk3,pk4"));
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
    public void emptyHighWatermark() throws Exception {
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
    public void filteredEvents() throws Exception {
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
    public void inserts4Pks() throws Exception {
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
    public void inserts4PksWithSignalFile() throws Exception {
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
    public void aSignalAddedToFileWhenConnectorIsStoppedShouldBeProcessedWhenItStarts() throws Exception {
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
    public void testPauseDuringSnapshotKafkaSignal() throws Exception {
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
