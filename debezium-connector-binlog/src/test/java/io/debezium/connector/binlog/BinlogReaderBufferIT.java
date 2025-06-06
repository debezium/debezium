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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
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

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        RO_DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    public void shouldCorrectlyManageRollback() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with("jdbc.creds.provider.user", "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with("jdbc.creds.provider.password", "snapperpass")
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
        if (replicaIsMaster) {
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
    public void shouldProcessSavepoint() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with("jdbc.creds.provider.user", "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with("jdbc.creds.provider.password", "snapperpass")
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
    public void shouldProcessLargeTransaction() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with("jdbc.creds.provider.user", "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with("jdbc.creds.provider.password", "snapperpass")
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

    @FixFor("DBZ-411")
    @Test
    public void shouldProcessRolledBackSavepoint() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with("jdbc.creds.provider.user", "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with("jdbc.creds.provider.password", "snapperpass")
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
        if (replicaIsMaster) {
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
