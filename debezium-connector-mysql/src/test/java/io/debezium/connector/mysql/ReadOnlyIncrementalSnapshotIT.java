/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.junit.SkipTestDependingOnGtidModeRule;
import io.debezium.connector.mysql.junit.SkipWhenGtidModeIs;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.kafka.KafkaCluster;
import io.debezium.pipeline.signal.channels.FileSignalChannel;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

@SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.OFF, reason = "Read only connection requires GTID_MODE to be ON")
public class ReadOnlyIncrementalSnapshotIT extends IncrementalSnapshotIT {

    private static KafkaCluster kafka;
    private static final int PARTITION_NO = 0;
    public static final String EXCLUDED_TABLE = "b";
    @Rule
    public TestRule skipTest = new SkipTestDependingOnGtidModeRule();
    private final Path signalsFile = Paths.get("src", "test", "resources").resolve("debezium_signaling_file.txt");

    @Before
    public void before() throws SQLException {
        super.before();
        kafka.createTopic(getSignalsTopic(), 1, 1);
    }

    @BeforeClass
    public static void startKafka() throws Exception {
        File dataDir = Testing.Files.createTestingDirectory("signal_cluster");
        Testing.Files.delete(dataDir);
        kafka = new KafkaCluster().usingDirectory(dataDir)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .addBrokers(1)
                .withKafkaConfiguration(Collect.propertiesOf(
                        "auto.create.topics.enable", "false",
                        "zookeeper.session.timeout.ms", "20000"))
                .startup();
    }

    @AfterClass
    public static void stopKafka() {
        if (kafka != null) {
            kafka.shutdown();
        }
    }

    protected Configuration.Builder config() {
        return super.config()
                .with(MySqlConnectorConfig.TABLE_EXCLUDE_LIST, DATABASE.getDatabaseName() + "." + EXCLUDED_TABLE)
                .with(MySqlConnectorConfig.READ_ONLY_CONNECTION, true)
                .with(KafkaSignalChannel.SIGNAL_TOPIC, getSignalsTopic())
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafka.brokerList())
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source,kafka")
                .with(MySqlConnectorConfig.INCLUDE_SQL_QUERY, true)
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
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokerList())
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

    @Test
    public void insertsWithoutPks() throws Exception {
        // Testing.Print.enable();

        populate4WithoutPkTable();
        startConnector();

        sendExecuteSnapshotKafkaSignal(DATABASE.qualifiedTableName("a42"));

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32("pk1") * 1_000 + k.getInt32("pk2") * 100 + k.getInt32("pk3") * 10 + k.getInt32("pk4"),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                DATABASE.topicForTable("a42"),
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test(expected = ConnectException.class)
    @SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.ON, reason = "Read only connection requires GTID_MODE to be ON")
    public void shouldFailIfGtidModeIsOff() throws Exception {
        // Testing.Print.enable();
        populateTable();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        startConnector((success, message, error) -> exception.set(error));
        waitForConnectorShutdown("mysql", DATABASE.getServerName());
        stopConnector();
        final Throwable e = exception.get();
        if (e != null) {
            throw (RuntimeException) e;
        }
    }

    @Test
    @FixFor("DBZ-5453")
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

        stopConnector();
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

    private void sendExecuteSnapshotFileSignal(String fullTableNames) throws IOException {

        String signalValue = String.format(
                "{\"id\":\"12345\",\"type\":\"execute-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}}",
                fullTableNames);

        java.nio.file.Files.write(signalsFile, signalValue.getBytes());

    }

    protected void populate4PkTable() throws SQLException {
        try (JdbcConnection connection = databaseConnection()) {
            populate4PkTable(connection, "a4");
        }
    }

    protected void populate4WithoutPkTable() throws SQLException {
        try (JdbcConnection connection = databaseConnection()) {
            populate4PkTable(connection, "a42");
        }
    }
}
