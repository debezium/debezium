/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Jiri Pechanec, Randall Hauch
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class BinlogReaderBufferIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("myServer1", "connector_test")
            .withDbHistoryPath(DB_HISTORY_PATH);
    private final UniqueDatabase RO_DATABASE = new UniqueDatabase("myServer2", "connector_test_ro", DATABASE)
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        RO_DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
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
        config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(MySqlConnectorConfig.USER, "snapper")
                .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.SERVER_NAME, DATABASE.getServerName())
                .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 10_000)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with rollback
        // supported only for non-GTID setup
        // ---------------------------------------------------------------------------------------------------------------
        if (replicaIsMaster) {
            try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
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
                        if (Testing.Print.isEnabled()) {
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
        config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(MySqlConnectorConfig.USER, "snapper")
                .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.SERVER_NAME, DATABASE.getServerName())
                .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement
        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with savepoint
        // ---------------------------------------------------------------------------------------------------------------
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                final Connection jdbc = connection.connection();
                connection.setAutoCommit(false);
                final Statement statement = jdbc.createStatement();
                statement.executeUpdate("INSERT INTO customers VALUES(default, 'first', 'first', 'first')");
                jdbc.setSavepoint();
                statement.executeUpdate("INSERT INTO customers VALUES(default, 'second', 'second', 'second')");
                jdbc.commit();
                connection.query("SELECT * FROM customers", rs -> {
                    if (Testing.Print.isEnabled()) {
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
        config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(MySqlConnectorConfig.USER, "snapper")
                .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.SERVER_NAME, DATABASE.getServerName())
                .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 9)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement

        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with many events
        // ---------------------------------------------------------------------------------------------------------------
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
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
                    if (Testing.Print.isEnabled()) {
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
        config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(MySqlConnectorConfig.USER, "snapper")
                .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.SERVER_NAME, DATABASE.getServerName())
                .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement
        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Transaction with rollback to savepoint
        // supported only for non-GTID setup
        // ---------------------------------------------------------------------------------------------------------------
        if (replicaIsMaster) {
            try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
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
                        if (Testing.Print.isEnabled()) {
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
            if (MySqlTestConnection.isMySQL5() && !MySqlTestConnection.isPerconaServer()) {
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
