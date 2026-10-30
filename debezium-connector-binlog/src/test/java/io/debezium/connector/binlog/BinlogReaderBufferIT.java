/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.junit.SkipWhenDatabaseIs;
import io.debezium.connector.binlog.junit.SkipWhenGtidModeIs;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.engine.DebeziumEngine;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Testing;

/**
 * @author Jiri Pechanec, Randall Hauch
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public abstract class BinlogReaderBufferIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", "connector_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);
    private final UniqueDatabase RO_DATABASE = TestHelper.getUniqueDatabase("myServer2", "connector_test_ro", DATABASE)
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        RO_DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MYSQL, reason = "MariaDB-specific GTID behavior")
    @SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.PERCONA, reason = "MariaDB-specific GTID behavior")
    @Test
    @FixFor("debezium/dbz#2470")
    void shouldPropagateGtidForStandaloneDdl() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(BinlogConnectorConfig.SERVER_ID, 18765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 10_000)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        start(getConnectorClass(), config);
        consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1);

        final String currentGtidSet;
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute("ALTER TABLE products ADD COLUMN volume FLOAT");
            currentGtidSet = connection.queryAndMap("SELECT @@GLOBAL.gtid_binlog_pos", rs -> {
                if (rs.next()) {
                    return rs.getString(1);
                }
                throw new IllegalStateException("Could not get GTID set");
            });
        }

        final SourceRecords records = consumeRecordsByTopic(1);
        final SourceRecord ddlRecord = records.recordsForTopic(DATABASE.getServerName()).get(0);
        final Struct source = ((Struct) ddlRecord.value()).getStruct("source");
        final String ddlGtid = source.getString(BinlogSourceInfo.GTID_KEY);

        assertThat(ddlGtid).isNotNull();
        assertThat(currentGtidSet.split(",")).contains(ddlGtid);
    }

    @Test
    void shouldCorrectlyManageRollback() throws SQLException, InterruptedException {
        String primaryPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsPrimary = primaryPort.equals(replicaPort);
        if (!replicaIsPrimary) {
            // Give time for the replica to catch up to the primary ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "primary" ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(BinlogConnectorConfig.SERVER_ID, 18765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 10_000)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with rollback
        // supported only for non-GTID setup
        // ---------------------------------------------------------------------------------------------------------------
        if (replicaIsPrimary) {
            try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
                try (JdbcConnection connection = db.connect()) {
                    final Connection jdbc = connection.connection();
                    connection.setAutoCommit(false);
                    final Statement statement = jdbc.createStatement();
                    statement.executeUpdate("CREATE TEMPORARY TABLE tmp_ids (a int)");
                    statement.executeUpdate("INSERT INTO tmp_ids VALUES(5)");
                    jdbc.commit();
                    statement.executeUpdate("DROP TEMPORARY TABLE tmp_ids");
                    statement.executeUpdate("UPDATE products SET weight=100.12 WHERE id=109");
                    jdbc.rollback();
                    connection.query("SELECT * FROM products", rs -> {
                        if (Print.isEnabled()) {
                            connection.print(rs);
                        }
                    });
                    connection.setAutoCommit(true);
                }
            }

            // The rolled-back transaction should be skipped
            Thread.sleep(5000);

            // Try to read records and verify that there is none
            assertNoRecordsToConsume();
            assertEngineIsRunning();

            Testing.print("*** Done with rollback TX");
        }
    }

    @Test
    void shouldProcessSavepoint() throws SQLException, InterruptedException {
        String primaryPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsPrimary = primaryPort.equals(replicaPort);
        if (!replicaIsPrimary) {
            // Give time for the replica to catch up to the primary ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "primary" ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(BinlogConnectorConfig.SERVER_ID, 18765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement
        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with savepoint
        // ---------------------------------------------------------------------------------------------------------------
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                final Connection jdbc = connection.connection();
                connection.setAutoCommit(false);
                final Statement statement = jdbc.createStatement();
                statement.executeUpdate("INSERT INTO customers VALUES(default, 'first', 'first', 'first')");
                jdbc.setSavepoint();
                statement.executeUpdate("INSERT INTO customers VALUES(default, 'second', 'second', 'second')");
                jdbc.commit();
                connection.query("SELECT * FROM customers", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
                connection.setAutoCommit(true);
            }
        }

        // 2 INSERTS, SAVEPOINT is filtered
        records = consumeRecordsByTopic(2);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("customers"))).hasSize(2);
        assertThat(records.allRecordsInOrder()).hasSize(2);
        Testing.print("*** Done with savepoint TX");
    }

    @Test
    void shouldProcessLargeTransaction() throws SQLException, InterruptedException {
        String primaryPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsPrimary = primaryPort.equals(replicaPort);
        if (!replicaIsPrimary) {
            // Give time for the replica to catch up to the primary ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "primary" ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(BinlogConnectorConfig.SERVER_ID, 18765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 9)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement

        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with many events
        // ---------------------------------------------------------------------------------------------------------------
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            final int numRecords = 40;
            try (JdbcConnection connection = db.connect()) {
                final Connection jdbc = connection.connection();
                connection.setAutoCommit(false);
                final Statement statement = jdbc.createStatement();
                for (int i = 0; i < numRecords; i++) {
                    statement.executeUpdate(String.format(
                            "INSERT INTO customers\n" + "VALUES (default,\"%s\",\"%s\",\"%s\")", i, i, i));
                }
                jdbc.commit();
                connection.query("SELECT * FROM customers", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
                connection.setAutoCommit(true);
            }

            // All records should be present only once
            records = consumeRecordsByTopic(numRecords);
            int recordIndex = 0;
            for (SourceRecord r : records.allRecordsInOrder()) {
                Struct envelope = (Struct) r.value();
                assertThat(envelope.getString("op")).isEqualTo(("c"));
                assertThat(envelope.getStruct("after").getString("email")).isEqualTo(Integer.toString(recordIndex++));
            }
            assertThat(records.topics().size()).isEqualTo(1);

            Testing.print("*** Done with large TX");
        }
    }

    @Test
    @FixFor("debezium/dbz#2914")
    @SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.OFF, reason = "The rest of an oversized transaction is lost on the buffer rewind only when GTIDs are used")
    @SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MARIADB, reason = "The rewind change targets the MySQL GTID dump command")
    void shouldProcessLargeTransactionCompletelyAndOnlyOnceWithGtids() throws SQLException, InterruptedException {
        String primaryPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsPrimary = primaryPort.equals(replicaPort);
        if (!replicaIsPrimary) {
            // Give time for the replica to catch up to the primary ...
            Thread.sleep(5000L);
        }

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(BinlogConnectorConfig.SERVER_ID, 18765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 9)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        start(getConnectorClass(), config);

        consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement

        // The transaction has many more events than the buffer holds, so the part that did not fit is re-read
        // from the binlog. In GTID mode that re-read must not make the server skip the transaction.
        final int numRecords = 40;
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            final Connection jdbc = connection.connection();
            connection.setAutoCommit(false);
            final Statement statement = jdbc.createStatement();
            for (int i = 0; i < numRecords; i++) {
                statement.executeUpdate(String.format("INSERT INTO customers VALUES (default,\"%s\",\"%s\",\"%s\")", i, i, i));
            }
            jdbc.commit();
            connection.setAutoCommit(true);

            // All records of the transaction must be present, in order
            SourceRecords records = consumeRecordsByTopic(numRecords);
            assertThat(records.allRecordsInOrder()).hasSize(numRecords);
            int recordIndex = 0;
            for (SourceRecord r : records.allRecordsInOrder()) {
                Struct envelope = (Struct) r.value();
                assertThat(envelope.getString("op")).isEqualTo("c");
                assertThat(envelope.getStruct("after").getString("email")).isEqualTo(Integer.toString(recordIndex++));
            }

            // A following transaction must be the next thing that arrives: nothing of the large transaction may be
            // emitted a second time, and the buffer must have been reset after the rewind
            connection.execute("INSERT INTO customers VALUES (default, 'after', 'after', 'after')");
            records = consumeRecordsByTopic(1);
            assertThat(records.allRecordsInOrder()).hasSize(1);
            final Struct after = ((Struct) records.allRecordsInOrder().get(0).value()).getStruct("after");
            assertThat(after.getString("email")).isEqualTo("after");
            assertNoRecordsToConsume();
        }
    }

    @Test
    @SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.OFF, reason = "Every rewind re-reads the transaction with the GTID dump command")
    @SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MARIADB, reason = "The rewind change targets the MySQL GTID dump command")
    void shouldKeepStreamingAfterManyBufferRewindsWithGtids() throws SQLException, InterruptedException {
        String primaryPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsPrimary = primaryPort.equals(replicaPort);
        if (!replicaIsPrimary) {
            // Give time for the replica to catch up to the primary ...
            Thread.sleep(5000L);
        }

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(BinlogConnectorConfig.SERVER_ID, 18765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 2)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        start(getConnectorClass(), config);

        consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement

        // Each transaction rewinds. The read loops of the rewinds must not nest, or the stack overflows after a few
        // hundred of them and the connector stops emitting
        final int transactions = 1_000;
        final int rowsPerTransaction = 3;
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            final Connection jdbc = connection.connection();
            connection.setAutoCommit(false);
            final Statement statement = jdbc.createStatement();
            int expectedEmail = 0;
            for (int t = 0; t < transactions; t++) {
                for (int r = 0; r < rowsPerTransaction; r++) {
                    final int id = t * rowsPerTransaction + r;
                    statement.executeUpdate(String.format("INSERT INTO customers VALUES (default,\"%s\",\"%s\",\"%s\")", id, id, id));
                }
                jdbc.commit();

                // every transaction must arrive complete and in order
                final SourceRecords records = consumeRecordsByTopic(rowsPerTransaction);
                assertThat(records.allRecordsInOrder()).hasSize(rowsPerTransaction);
                for (SourceRecord record : records.allRecordsInOrder()) {
                    final Struct after = ((Struct) record.value()).getStruct("after");
                    assertThat(after.getString("email")).isEqualTo(Integer.toString(expectedEmail++));
                }
            }
            connection.setAutoCommit(true);
            assertNoRecordsToConsume();
        }
    }

    @Test
    @SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.OFF, reason = "Every rewind re-reads the transaction with the GTID dump command")
    @SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MARIADB, reason = "The rewind change targets the MySQL GTID dump command")
    void shouldStopWhileBufferRewindsAreRunning() throws SQLException, InterruptedException {
        startWithBufferOfTwo(null);
        consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement

        // Queue up enough transactions that the connector is rewinding when it is stopped
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            final Connection jdbc = connection.connection();
            connection.setAutoCommit(false);
            final Statement statement = jdbc.createStatement();
            for (int t = 0; t < 300; t++) {
                for (int r = 0; r < 3; r++) {
                    final int id = t * 3 + r;
                    statement.executeUpdate(String.format("INSERT INTO customers VALUES (default,\"%s\",\"%s\",\"%s\")", id, id, id));
                }
                jdbc.commit();
            }
            connection.setAutoCommit(true);
        }
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(this::isRewindThreadAlive);

        stopConnector();

        // the rewind thread must not outlive the connector
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> !isRewindThreadAlive());
    }

    @Test
    @SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.OFF, reason = "Every rewind re-reads the transaction with the GTID dump command")
    @SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MARIADB, reason = "The rewind change targets the MySQL GTID dump command")
    void shouldFailTheTaskWhenReconnectingForARewindFails() throws SQLException, InterruptedException {
        final AtomicReference<Boolean> success = new AtomicReference<>();
        startWithBufferOfTwo((s, message, error) -> success.set(s));
        consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement

        // The connected client keeps working, but the reconnect of the rewind cannot log in, and nothing else reads the binlog
        try (BinlogTestConnection admin = getTestDatabaseConnection("mysql");
                JdbcConnection adminConnection = admin.connect()) {
            adminConnection.execute("ALTER USER 'snapper'@'%' ACCOUNT LOCK");
            try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                    JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO customers VALUES (default, 'a', 'a', 'a')",
                        "INSERT INTO customers VALUES (default, 'b', 'b', 'b')",
                        "INSERT INTO customers VALUES (default, 'c', 'c', 'c')");
                Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> success.get() != null);
                assertThat(success.get()).isFalse();
            }
            finally {
                adminConnection.execute("ALTER USER 'snapper'@'%' ACCOUNT UNLOCK");
            }
        }
    }

    private void startWithBufferOfTwo(DebeziumEngine.CompletionCallback callback) throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(BinlogConnectorConfig.SERVER_ID, 18765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 2)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();
        if (callback == null) {
            start(getConnectorClass(), config);
        }
        else {
            start(getConnectorClass(), config, callback);
        }
    }

    private boolean isRewindThreadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.isAlive() && t.getName().endsWith("-binlog-rewind") && t.getName().contains(DATABASE.getServerName()));
    }

    @FixFor("DBZ-411")
    @Test
    void shouldProcessRolledBackSavepoint() throws SQLException, InterruptedException {
        String primaryPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsPrimary = primaryPort.equals(replicaPort);
        if (!replicaIsPrimary) {
            // Give time for the replica to catch up to the primary ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "primary" ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with(BinlogConnectorConfig.SERVER_ID, 18765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement
        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with rollback to savepoint
        // supported only for non-GTID setup
        // ---------------------------------------------------------------------------------------------------------------
        if (replicaIsPrimary) {
            try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
                try (JdbcConnection connection = db.connect()) {
                    final Connection jdbc = connection.connection();
                    connection.setAutoCommit(false);
                    final Statement statement = jdbc.createStatement();
                    statement.executeUpdate("CREATE TEMPORARY TABLE tmp_ids (a int)");
                    statement.executeUpdate("INSERT INTO tmp_ids VALUES(5)");
                    jdbc.commit();
                    statement.executeUpdate("DROP TEMPORARY TABLE tmp_ids");
                    statement.executeUpdate("INSERT INTO customers VALUES(default, 'first', 'first', 'first')");
                    final Savepoint savepoint = jdbc.setSavepoint();
                    statement.executeUpdate("INSERT INTO customers VALUES(default, 'second', 'second', 'second')");
                    jdbc.rollback(savepoint);
                    jdbc.commit();
                    connection.query("SELECT * FROM customers", rs -> {
                        if (Print.isEnabled()) {
                            connection.print(rs);
                        }
                    });
                    connection.setAutoCommit(true);
                }
            }

            // Bug DBZ-533
            int recordCount;
            int customerEventsCount;
            int topicCount;
            if (isMySQL5() && !isPerconaServer()) {
                // MySQL 5 contains events when the TX was effectively rolled-back
                // INSERT + INSERT + ROLLBACK, SAVEPOINT filtered
                recordCount = 3;
                customerEventsCount = 2;
                topicCount = 2;
            }
            else {
                // MySQL 8 does not propagate rolled back changes
                // INSERT, SAVEPOINT filtered
                recordCount = 1;
                customerEventsCount = 1;
                topicCount = 1;
            }
            records = consumeRecordsByTopic(recordCount);
            assertThat(records.topics().size()).isEqualTo(topicCount);
            assertThat(records.recordsForTopic(DATABASE.topicForTable("customers"))).hasSize(customerEventsCount);
            assertThat(records.allRecordsInOrder()).hasSize(recordCount);
            Testing.print("*** Done with savepoint TX");
        }
    }
}
