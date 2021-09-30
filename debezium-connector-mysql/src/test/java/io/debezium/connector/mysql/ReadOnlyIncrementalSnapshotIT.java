/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.File;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.fest.assertions.Assertions;
import org.fest.assertions.MapAssert;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.junit.SkipTestDependingOnGtidModeRule;
import io.debezium.connector.mysql.junit.SkipWhenGtidModeIs;
import io.debezium.connector.mysql.signal.KafkaSignalThread;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

@SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.OFF, reason = "Read only connection requires GTID_MODE to be ON")
public class ReadOnlyIncrementalSnapshotIT extends IncrementalSnapshotIT {

    private static KafkaCluster kafka;
    private static final int PARTITION_NO = 0;
    public static final String EXCLUDED_TABLE = "b";
    @Rule
    public TestRule skipTest = new SkipTestDependingOnGtidModeRule();

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
                .with(KafkaSignalThread.SIGNAL_TOPIC, getSignalsTopic())
                .with(KafkaSignalThread.BOOTSTRAP_SERVERS, kafka.brokerList());
    }

    private String getSignalsTopic() {
        return DATABASE.getDatabaseName() + "signals_topic";
    }

    protected void sendExecuteSnapshotKafkaSignal() throws ExecutionException, InterruptedException {
        String signalValue = String.format(
                "{\"type\":\"execute-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}}",
                tableDataCollectionId());
        final ProducerRecord<String, String> executeSnapshotSignal = new ProducerRecord<>(getSignalsTopic(), PARTITION_NO, SERVER_NAME, signalValue);

        final Configuration signalProducerConfig = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokerList())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "signals")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(signalProducerConfig.asProperties())) {
            producer.send(executeSnapshotSignal).get();
        }
    }

    @Test
    public void emptyHighWatermark() throws Exception {
        Testing.Print.enable();

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
    public void filteredEvents() throws Exception {
        Testing.Print.enable();

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
                Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
            }
        }
        finally {
            t.interrupt();
        }
    }

    @Test(expected = ConnectException.class)
    @SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.ON, reason = "Read only connection requires GTID_MODE to be ON")
    public void shouldFailIfGtidModeIsOff() throws Exception {
        Testing.Print.enable();
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
}
