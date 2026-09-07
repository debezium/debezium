/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
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
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.kafka.KafkaClusterUtils;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.testing.testcontainers.ImageNames;
import io.debezium.util.Testing;
import io.strimzi.test.container.StrimziKafkaCluster;

/**
 * <p>This test uses Testcontainers to start a dedicated MySQL 8.4+ instance with
 * {@code gtid_mode=ON} so it can be executed in complete isolation, independent of
 * any pre-configured Docker container managed by the Maven build profile.
 *
 * <h3>Bug description</h3>
 * {@link MySqlReadOnlyIncrementalSnapshotContext#reachedHighWatermark(String)} previously
 * looked up the watermark {@code UUIDSet} by server UUID <em>only</em>, ignoring the GTID
 * tag.  When the high-watermark GTID carried a tag, the lookup returned {@code null} and
 * the subsequent {@code OptionalLong.getAsLong()} threw {@link java.util.NoSuchElementException},
 * crashing the connector task mid-snapshot.
 *
 */
public class MySqlTaggedGtidWatermarkIT extends AbstractAsyncEngineConnectorTest {

    /** MySQL 9.7 supports tagged GTIDs */
    private static final String MYSQL_IMAGE = ImageNames.MYSQL_9_7_IMAGE;

    private static final String DB_NAME = "gtid_watermark_test";
    private static final String TABLE_NAME = "a";
    private static final String SERVER_NAME = "gtid_watermark_server";
    private static final String SIGNALS_TOPIC = DB_NAME + "_signals";
    private static final String GTID_TAG = "dbz_is_test";
    private static final String DB_USER = "dbzadmin";
    private static final String DB_PASSWORD = "dbzpass";

    /** Number of rows used as the incremental-snapshot source. */
    private static final int ROW_COUNT = 100;

    private static MySQLContainer mysql;
    private static StrimziKafkaCluster kafkaCluster;

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-gtid-watermark-it.txt").toAbsolutePath();

    @BeforeAll
    static void startInfrastructure() throws Exception {
        final Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put("auto.create.topics.enable", "false");
        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withAdditionalKafkaConfiguration(kafkaProps)
                .withSharedNetwork()
                .build();
        kafkaCluster.start();
        KafkaClusterUtils.createTopic(SIGNALS_TOPIC, 1, (short) 1, kafkaCluster.getBootstrapServers());

        // MySQLContainer creates DB_USER with DB_PASSWORD and grants it full access to DB_NAME.
        // That same account is used for DML setup, tagged inserts, and the Debezium connector,
        // avoiding any need to reach the root account from outside the container.
        mysql = new MySQLContainer(
                DockerImageName.parse(MYSQL_IMAGE).asCompatibleSubstituteFor("mysql"))
                .withDatabaseName(DB_NAME)
                .withUsername(DB_USER)
                .withPassword(DB_PASSWORD)
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("docker/server-tagged-gtid/my.cnf"),
                        "/etc/my.cnf")
                .withStartupTimeout(Duration.ofMinutes(3));
        mysql.start();

        // Grant the replication privileges that MySQLContainer does not add by default.
        // Must run as root inside the container — dbzadmin has no GRANT OPTION on *.*.
        // MySQLContainer sets MYSQL_ROOT_PASSWORD equal to the supplied user password.
        // TRANSACTION_GTID_TAG + SESSION_VARIABLES_ADMIN are required to SET gtid_next = 'AUTOMATIC:<tag>'.
        mysql.execInContainer("mysql", "-uroot", "-p" + DB_PASSWORD,
                "-e", "GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT,"
                        + " TRANSACTION_GTID_TAG, SESSION_VARIABLES_ADMIN ON *.* TO '"
                        + DB_USER + "'@'%'; FLUSH PRIVILEGES;");

        try (var conn = adminConnection()) {
            try (var st = conn.createStatement()) {
                st.execute(
                        "CREATE TABLE IF NOT EXISTS `" + DB_NAME + "`.`" + TABLE_NAME + "`"
                                + " (pk INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY, aa INTEGER)"
                                + " AUTO_INCREMENT = 1");
            }
        }
    }

    @AfterAll
    static void stopInfrastructure() {
        if (kafkaCluster != null) {
            kafkaCluster.stop();
        }
        if (mysql != null) {
            mysql.stop();
        }
    }

    @BeforeEach
    void before() throws Exception {
        try (var conn = adminConnection()) {
            try (var st = conn.createStatement()) {
                st.execute("USE `" + DB_NAME + "`");
                st.execute("DELETE FROM " + TABLE_NAME);
                st.execute("ALTER TABLE " + TABLE_NAME + " AUTO_INCREMENT = 1");
            }
        }
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void after() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("debezium/dbz#2496")
    void shouldCompleteIncrementalSnapshotWhenHighWatermarkContainsTaggedGtid() throws Exception {
        // Step 1 – populate the snapshot-source table (untagged transactions).
        try (var conn = adminConnection()) {
            try (var st = conn.createStatement()) {
                st.execute("USE `" + DB_NAME + "`");
                for (int i = 1; i <= ROW_COUNT; i++) {
                    st.execute("INSERT INTO " + TABLE_NAME
                            + " (pk, aa) VALUES (" + i + ", " + (i - 1) + ")");
                }
            }
        }

        // Step 2 – start the connector in read-only mode; no initial snapshot (NO_DATA).
        start(MySqlConnector.class, connectorConfig());
        assertConnectorIsRunning();
        waitForStreamingRunning(Module.name(), SERVER_NAME);

        // Step 3 – trigger the incremental snapshot via a Kafka signal.
        sendExecuteSnapshotSignal(DB_NAME + "." + TABLE_NAME);

        // Step 4 – insert rows tagged with GTID_TAG.
        try (var conn = adminConnection()) {
            try (var st = conn.createStatement()) {
                st.execute("USE `" + DB_NAME + "`");
                for (int i = ROW_COUNT + 1; i <= ROW_COUNT + 30; i++) {
                    st.execute("SET @@SESSION.gtid_next = 'AUTOMATIC:" + GTID_TAG + "'");
                    st.execute("INSERT INTO " + TABLE_NAME
                            + " (pk, aa) VALUES (" + i + ", " + i + ")");
                    st.execute("SET @@SESSION.gtid_next = 'AUTOMATIC'");
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Tagged-GTID inserts failed", e);
        }

        // Step 5 – wait until we receive all ROW_COUNT original rows.
        //
        // The snapshot window can only close after reachedHighWatermark() returns true for
        // a GTID at or past the high watermark. If the fix is absent the connector task
        // fails, no records arrive, and the Awaitility assertion times out.
        final String topicName = SERVER_NAME + "." + DB_NAME + "." + TABLE_NAME;
        final List<SourceRecord> received = new ArrayList<>();

        Awaitility.await("incremental snapshot to complete")
                .atMost(3, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    consumeAvailableRecords(rec -> {
                        if (topicName.equals(rec.topic())) {
                            received.add(rec);
                        }
                    });
                    // We expect at least ROW_COUNT snapshot records (pk 1..ROW_COUNT).
                    final long snapshotRecords = received.stream()
                            .filter(r -> {
                                final Struct source = ((Struct) r.value()).getStruct("source");
                                return "incremental".equals(source.getString("snapshot"));
                            })
                            .count();
                    Assertions.assertThat(snapshotRecords).isGreaterThanOrEqualTo(ROW_COUNT);
                });

        // Verify all ROW_COUNT original rows were delivered.
        final Map<Integer, Integer> byPk = new HashMap<>();
        for (final SourceRecord rec : received) {
            final Struct key = (Struct) rec.key();
            final int pk = key.getInt32("pk");
            if (pk <= ROW_COUNT) {
                byPk.put(pk, ((Struct) rec.value()).getStruct("after").getInt32("aa"));
            }
        }
        for (int i = 1; i <= ROW_COUNT; i++) {
            Assertions.assertThat(byPk).containsEntry(i, i - 1);
        }
    }

    private Configuration connectorConfig() {
        return Configuration.create()
                // Connection
                .with(BinlogConnectorConfig.HOSTNAME, mysql.getHost())
                .with(BinlogConnectorConfig.PORT, mysql.getMappedPort(3306))
                .with(BinlogConnectorConfig.USER, DB_USER)
                .with(BinlogConnectorConfig.PASSWORD, DB_PASSWORD)
                .with(MySqlConnectorConfig.SSL_MODE, "disabled")
                .with("driver.allowPublicKeyRetrieval", "true")
                .with("database.connectionTimeZone", "UTC")
                // Identity
                .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .with(BinlogConnectorConfig.SERVER_ID, 33445)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DB_NAME)
                // Snapshot: skip the initial snapshot; we use read-only incremental snapshot only.
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA.getValue())
                // Read-only mode (GTID-based incremental snapshot).
                .with(BinlogConnectorConfig.READ_ONLY_CONNECTION, true)
                .with(BinlogConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                // Kafka signal channel for executing the snapshot.
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "kafka")
                .with(KafkaSignalChannel.SIGNAL_TOPIC, SIGNALS_TOPIC)
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers())
                // Schema history stored to a local file.
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .with(BinlogConnectorConfig.STORE_ONLY_CAPTURED_DATABASES_DDL, true)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();
    }

    private void sendExecuteSnapshotSignal(final String dataCollectionId)
            throws ExecutionException, InterruptedException {
        final String signalValue = String.format(
                "{\"type\":\"execute-snapshot\",\"data\":{\"data-collections\":[\"%s\"],\"type\":\"INCREMENTAL\"}}",
                dataCollectionId);
        final Configuration producerCfg = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "gtid-watermark-signal-producer")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerCfg.asProperties())) {
            producer.send(new ProducerRecord<>(SIGNALS_TOPIC, 0, SERVER_NAME, signalValue)).get();
        }
    }

    private static Connection adminConnection() throws Exception {
        final String url = String.format(
                "jdbc:mysql://%s:%d/%s?allowPublicKeyRetrieval=true&useSSL=false",
                mysql.getHost(), mysql.getMappedPort(3306), DB_NAME);
        return DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
    }
}
