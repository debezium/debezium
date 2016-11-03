/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.config.Field.Recommender;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.data.Envelope;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine.CompletionResult;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.relational.history.KafkaDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    /**
     * Verifies that the connector doesn't run with an invalid configuration. This does not actually connect to the MySQL server.
     */
    @Test
    public void shouldNotStartWithInvalidConfiguration() {
        config = Configuration.create()
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();

        // we expect the engine will log at least one error, so preface it ...
        logger.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exceptions will appear in the log");
        start(MySqlConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldFailToValidateInvalidConfiguration() {
        Configuration config = Configuration.create()
                                            .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                                            .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                                            .build();
        MySqlConnector connector = new MySqlConnector();
        Config result = connector.validate(config.asMap());

        assertConfigurationErrors(result, MySqlConnectorConfig.HOSTNAME, 1);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.PORT);
        assertConfigurationErrors(result, MySqlConnectorConfig.USER, 1);
        assertConfigurationErrors(result, MySqlConnectorConfig.PASSWORD, 1);
        assertConfigurationErrors(result, MySqlConnectorConfig.SERVER_NAME, 1);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SERVER_ID);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.TABLES_IGNORE_BUILTIN);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.DATABASE_WHITELIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.DATABASE_BLACKLIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.TABLE_WHITELIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.TABLE_BLACKLIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.COLUMN_BLACKLIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.CONNECTION_TIMEOUT_MS);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.KEEP_ALIVE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.MAX_QUEUE_SIZE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.MAX_BATCH_SIZE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.POLL_INTERVAL_MS);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.DATABASE_HISTORY);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SNAPSHOT_MODE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SNAPSHOT_MINIMAL_LOCKING);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_MODE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_KEYSTORE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_TRUSTSTORE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_TRUSTSTORE_PASSWORD);
        assertConfigurationErrors(result, KafkaDatabaseHistory.BOOTSTRAP_SERVERS);
        assertConfigurationErrors(result, KafkaDatabaseHistory.TOPIC);
        assertNoConfigurationErrors(result, KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS);
        assertNoConfigurationErrors(result, KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS);
    }

    @Test
    public void shouldValidateValidConfigurationWithSSL() {
        Configuration config = Configuration.create()
                                            .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                                            .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                                            .with(MySqlConnectorConfig.USER, "snapper")
                                            .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                                            .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.REQUIRED.name().toLowerCase())
                                            .with(MySqlConnectorConfig.SSL_KEYSTORE, "/some/path/to/keystore")
                                            .with(MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD, "keystore1234")
                                            .with(MySqlConnectorConfig.SSL_TRUSTSTORE, "/some/path/to/truststore")
                                            .with(MySqlConnectorConfig.SSL_TRUSTSTORE_PASSWORD, "truststore1234")
                                            .with(MySqlConnectorConfig.SERVER_ID, 18765)
                                            .with(MySqlConnectorConfig.SERVER_NAME, "myServer")
                                            .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, "some.host.com")
                                            .with(KafkaDatabaseHistory.TOPIC, "my.db.history.topic")
                                            .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                                            .build();
        MySqlConnector connector = new MySqlConnector();
        Config result = connector.validate(config.asMap());

        // Can't connect to MySQL using SSL on a container using the 'mysql/mysql-server' image maintained by MySQL team,
        // but can actually connect to MySQL using SSL on a container using the 'mysql' image maintained by Docker, Inc.
        assertConfigurationErrors(result, MySqlConnectorConfig.HOSTNAME, 0, 1);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.PORT);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.USER);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.PASSWORD);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SERVER_NAME);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SERVER_ID);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.TABLES_IGNORE_BUILTIN);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.DATABASE_WHITELIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.DATABASE_BLACKLIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.TABLE_WHITELIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.TABLE_BLACKLIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.COLUMN_BLACKLIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.CONNECTION_TIMEOUT_MS);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.KEEP_ALIVE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.MAX_QUEUE_SIZE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.MAX_BATCH_SIZE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.POLL_INTERVAL_MS);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.DATABASE_HISTORY);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SNAPSHOT_MODE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SNAPSHOT_MINIMAL_LOCKING);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_MODE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_KEYSTORE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_TRUSTSTORE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_TRUSTSTORE_PASSWORD);
        assertNoConfigurationErrors(result, KafkaDatabaseHistory.BOOTSTRAP_SERVERS);
        assertNoConfigurationErrors(result, KafkaDatabaseHistory.TOPIC);
        assertNoConfigurationErrors(result, KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS);
        assertNoConfigurationErrors(result, KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS);
    }

    @Test
    public void shouldValidateAcceptableConfiguration() {
        Configuration config = Configuration.create()
                                            .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                                            .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                                            .with(MySqlConnectorConfig.USER, "snapper")
                                            .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                                            .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                                            .with(MySqlConnectorConfig.SERVER_ID, 18765)
                                            .with(MySqlConnectorConfig.SERVER_NAME, "myServer")
                                            .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, "some.host.com")
                                            .with(KafkaDatabaseHistory.TOPIC, "my.db.history.topic")
                                            .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                                            .build();
        MySqlConnector connector = new MySqlConnector();
        Config result = connector.validate(config.asMap());

        assertNoConfigurationErrors(result, MySqlConnectorConfig.HOSTNAME);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.PORT);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.USER);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.PASSWORD);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SERVER_NAME);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SERVER_ID);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.TABLES_IGNORE_BUILTIN);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.DATABASE_WHITELIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.DATABASE_BLACKLIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.TABLE_WHITELIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.TABLE_BLACKLIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.COLUMN_BLACKLIST);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.CONNECTION_TIMEOUT_MS);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.KEEP_ALIVE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.MAX_QUEUE_SIZE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.MAX_BATCH_SIZE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.POLL_INTERVAL_MS);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.DATABASE_HISTORY);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SNAPSHOT_MODE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SNAPSHOT_MINIMAL_LOCKING);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_MODE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_KEYSTORE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_TRUSTSTORE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_TRUSTSTORE_PASSWORD);
        assertNoConfigurationErrors(result, KafkaDatabaseHistory.BOOTSTRAP_SERVERS);
        assertNoConfigurationErrors(result, KafkaDatabaseHistory.TOPIC);
        assertNoConfigurationErrors(result, KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS);
        assertNoConfigurationErrors(result, KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS);

        // Testing.Debug.enable();

        Recommender dbNameRecommender = MySqlConnectorConfig.DATABASE_WHITELIST.recommender();
        List<Object> dbNames = dbNameRecommender.validValues(MySqlConnectorConfig.DATABASE_WHITELIST, config);
        Testing.debug("List of dbNames: " + dbNames);
        assertThat(dbNames).containsOnly("connector_test", "readbinlog_test", "regression_test", "json_test",
                                         "connector_test_ro", "emptydb");

        Recommender tableNameRecommender = MySqlConnectorConfig.TABLE_WHITELIST.recommender();
        List<Object> tableNames = tableNameRecommender.validValues(MySqlConnectorConfig.TABLE_WHITELIST, config);
        Testing.debug("List of tableNames: " + tableNames);
        assertThat(tableNames).containsOnly("readbinlog_test.product",
                                            "readbinlog_test.purchased",
                                            "readbinlog_test.person",
                                            "connector_test.customers",
                                            "connector_test.orders",
                                            "connector_test.products",
                                            "connector_test.products_on_hand",
                                            "connector_test_ro.customers",
                                            "connector_test_ro.orders",
                                            "connector_test_ro.products",
                                            "connector_test_ro.products_on_hand",
                                            "regression_test.t1464075356413_testtable6",
                                            "regression_test.dbz_85_fractest",
                                            "regression_test.dbz84_integer_types_table",
                                            "regression_test.dbz_100_enumsettest",
                                            "regression_test.dbz_102_charsettest",
                                            "regression_test.dbz_114_zerovaluetest",
                                            "regression_test.dbz_123_bitvaluetest",
                                            "regression_test.dbz_104_customers",
                                            "json_test.dbz_126_jsontable");

        // Now set the whitelist to two databases ...
        Configuration config2 = config.edit()
                                      .with(MySqlConnectorConfig.DATABASE_WHITELIST, "connector_test,connector_test_ro")
                                      .build();

        List<Object> tableNames2 = tableNameRecommender.validValues(MySqlConnectorConfig.TABLE_WHITELIST, config2);
        assertThat(tableNames2).containsOnly("connector_test.customers",
                                             "connector_test.orders",
                                             "connector_test.products",
                                             "connector_test.products_on_hand",
                                             "connector_test_ro.customers",
                                             "connector_test_ro.orders",
                                             "connector_test_ro.products",
                                             "connector_test_ro.products_on_hand");
        Testing.debug("List of tableNames: " + tableNames2);
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingSnapshot() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port");
        String replicaPort = System.getProperty("database.replica.port");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port"))
                              .with(MySqlConnectorConfig.USER, "snapper")
                              .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, "myServer")
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "connector_test")
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement
        assertThat(records.recordsForTopic("myServer").size()).isEqualTo(12);
        assertThat(records.recordsForTopic("myServer.connector_test.products").size()).isEqualTo(9);
        assertThat(records.recordsForTopic("myServer.connector_test.products_on_hand").size()).isEqualTo(9);
        assertThat(records.recordsForTopic("myServer.connector_test.customers").size()).isEqualTo(4);
        assertThat(records.recordsForTopic("myServer.connector_test.orders").size()).isEqualTo(5);
        assertThat(records.topics().size()).isEqualTo(5);
        assertThat(records.databaseNames().size()).isEqualTo(2);
        assertThat(records.ddlRecordsForDatabase("connector_test").size()).isEqualTo(11);
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("").size()).isEqualTo(1);
        records.ddlRecordsForDatabase("connector_test").forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // Check that the last record has snapshots disabled in the offset, but not in the source
        List<SourceRecord> allRecords = records.allRecordsInOrder();
        SourceRecord last = allRecords.get(allRecords.size() - 1);
        SourceRecord secondToLast = allRecords.get(allRecords.size() - 2);
        assertThat(secondToLast.sourceOffset().containsKey(SourceInfo.SNAPSHOT_KEY)).isTrue();
        assertThat(last.sourceOffset().containsKey(SourceInfo.SNAPSHOT_KEY)).isFalse(); // not snapshot
        assertThat(((Struct) secondToLast.value()).getStruct(Envelope.FieldName.SOURCE).getBoolean(SourceInfo.SNAPSHOT_KEY)).isTrue();
        assertThat(((Struct) last.value()).getStruct(Envelope.FieldName.SOURCE).getBoolean(SourceInfo.SNAPSHOT_KEY)).isTrue();

        // ---------------------------------------------------------------------------------------------------------------
        // Stopping the connector does not lose events recorded when connector is not running
        // ---------------------------------------------------------------------------------------------------------------

        // Make sure there are no more events and then stop the connector ...
        waitForAvailableRecords(3, TimeUnit.SECONDS);
        int totalConsumed = consumeAvailableRecords(this::print);
        System.out.println("TOTAL CONSUMED = " + totalConsumed);
        // assertThat(totalConsumed).isEqualTo(0);
        stopConnector();

        // Make some changes to data only while the connector is stopped ...
        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.query("SELECT * FROM products", rs -> {
                    if (Testing.Print.isEnabled()) connection.print(rs);
                });
                connection.execute("INSERT INTO products VALUES (default,'robot','Toy robot',1.304);");
                connection.query("SELECT * FROM products", rs -> {
                    if (Testing.Print.isEnabled()) connection.print(rs);
                });
            }
        }

        // Testing.Print.enable();

        // Restart the connector and read the insert record ...
        Testing.print("*** Restarting connector after inserts were made");
        start(MySqlConnector.class, config);
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("myServer.connector_test.products").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1);
        List<SourceRecord> inserts = records.recordsForTopic("myServer.connector_test.products");
        assertInsert(inserts.get(0), "id", 110);
        Testing.print("*** Done with inserts and restart");

        Testing.print("*** Stopping connector");
        stopConnector();
        Testing.print("*** Restarting connector");
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Simple INSERT
        // ---------------------------------------------------------------------------------------------------------------
        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO products VALUES (1001,'roy','old robot',1234.56);");
                connection.query("SELECT * FROM products", rs -> {
                    if (Testing.Print.isEnabled()) connection.print(rs);
                });
            }
        }

        // And consume the one insert ...
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("myServer.connector_test.products").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1);
        inserts = records.recordsForTopic("myServer.connector_test.products");
        assertInsert(inserts.get(0), "id", 1001);

        // Testing.print("*** Done with simple insert");

        // ---------------------------------------------------------------------------------------------------------------
        // Changing the primary key of a row should result in 3 events: INSERT, DELETE, and TOMBSTONE
        // ---------------------------------------------------------------------------------------------------------------
        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE products SET id=2001, description='really old robot' WHERE id=1001");
                connection.query("SELECT * FROM products", rs -> {
                    if (Testing.Print.isEnabled()) connection.print(rs);
                });
            }
        }
        // And consume the update of the PK, which is one insert followed by a delete followed by a tombstone ...
        records = consumeRecordsByTopic(3);
        List<SourceRecord> updates = records.recordsForTopic("myServer.connector_test.products");
        assertThat(updates.size()).isEqualTo(3);
        assertInsert(updates.get(0), "id", 2001);
        assertDelete(updates.get(1), "id", 1001);
        assertTombstone(updates.get(2), "id", 1001);

        Testing.print("*** Done with PK change");

        // ---------------------------------------------------------------------------------------------------------------
        // Simple UPDATE (with no schema changes)
        // ---------------------------------------------------------------------------------------------------------------
        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE products SET weight=1345.67 WHERE id=2001");
                connection.query("SELECT * FROM products", rs -> {
                    if (Testing.Print.isEnabled()) connection.print(rs);
                });
            }
        }

        // And consume the one update ...
        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        updates = records.recordsForTopic("myServer.connector_test.products");
        assertThat(updates.size()).isEqualTo(1);
        assertUpdate(updates.get(0), "id", 2001);
        updates.forEach(this::validate);

        Testing.print("*** Done with simple update");

        //Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Change our schema with a fully-qualified name; we should still see this event
        // ---------------------------------------------------------------------------------------------------------------
        // Add a column with default to the 'products' table and explicitly update one record ...
        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("ALTER TABLE connector_test.products ADD COLUMN volume FLOAT, ADD COLUMN alias VARCHAR(30) NULL AFTER description");
                connection.execute("UPDATE products SET volume=13.5 WHERE id=2001");
                connection.query("SELECT * FROM products", rs -> {
                    if (Testing.Print.isEnabled()) connection.print(rs);
                });
            }
        }

        // And consume the one schema change event and one update event ...
        records = consumeRecordsByTopic(2);
        assertThat(records.topics().size()).isEqualTo(2);
        assertThat(records.recordsForTopic("myServer").size()).isEqualTo(1);
        updates = records.recordsForTopic("myServer.connector_test.products");
        assertThat(updates.size()).isEqualTo(1);
        assertUpdate(updates.get(0), "id", 2001);
        updates.forEach(this::validate);

        Testing.print("*** Done with schema change (same db and fully-qualified name)");

        // ---------------------------------------------------------------------------------------------------------------
        // DBZ-55 Change our schema using a different database and a fully-qualified name; we should still see this event
        // ---------------------------------------------------------------------------------------------------------------
        // Connect to a different database, but use the fully qualified name for a table in our database ...
        try (MySQLConnection db = MySQLConnection.forTestDatabase("emptydb");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("CREATE TABLE connector_test.stores ("
                        + " id INT(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,"
                        + " first_name VARCHAR(255) NOT NULL,"
                        + " last_name VARCHAR(255) NOT NULL,"
                        + " email VARCHAR(255) NOT NULL );");
            }
        }

        // And consume the one schema change event only ...
        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic("myServer").size()).isEqualTo(1);
        records.recordsForTopic("myServer").forEach(this::validate);

        Testing.print("*** Done with PK change (different db and fully-qualified name)");

        // ---------------------------------------------------------------------------------------------------------------
        // Make sure there are no additional events
        // ---------------------------------------------------------------------------------------------------------------

        // Do something completely different with a table we've not modified yet and then read that event.
        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE products_on_hand SET quantity=20 WHERE product_id=109");
                connection.query("SELECT * FROM products_on_hand", rs -> {
                    if (Testing.Print.isEnabled()) connection.print(rs);
                });
            }
        }

        // And make sure we consume that one update ...
        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        updates = records.recordsForTopic("myServer.connector_test.products_on_hand");
        assertThat(updates.size()).isEqualTo(1);
        assertUpdate(updates.get(0), "product_id", 109);
        updates.forEach(this::validate);

        Testing.print("*** Done with verifying no additional events");

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector ...
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();

        // ---------------------------------------------------------------------------------------------------------------
        // Restart the connector to read only part of a transaction ...
        // ---------------------------------------------------------------------------------------------------------------
        Testing.print("*** Restarting connector");
        CompletionResult completion = new CompletionResult();
        start(MySqlConnector.class, config, completion, (record) -> {
            // We want to stop before processing record 3003 ...
            Struct key = (Struct) record.key();
            Number id = (Number) key.get("id");
            if (id.intValue() == 3003) {
                return true;
            }
            return false;
        });

        BinlogPosition positionBeforeInserts = new BinlogPosition();
        BinlogPosition positionAfterInserts = new BinlogPosition();
        BinlogPosition positionAfterUpdate = new BinlogPosition();
        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.query("SHOW MASTER STATUS", positionBeforeInserts::readFromDatabase);
                connection.execute("INSERT INTO products(id,name,description,weight,volume,alias) VALUES "
                        + "(3001,'ashley','super robot',34.56,0.00,'ashbot'), "
                        + "(3002,'arthur','motorcycle',87.65,0.00,'arcycle'), "
                        + "(3003,'oak','tree',987.65,0.00,'oak');");
                connection.query("SELECT * FROM products", rs -> {
                    if (Testing.Print.isEnabled()) connection.print(rs);
                });
                connection.query("SHOW MASTER STATUS", positionAfterInserts::readFromDatabase);
                // Change something else that is unrelated ...
                connection.execute("UPDATE products_on_hand SET quantity=40 WHERE product_id=109");
                connection.query("SELECT * FROM products_on_hand", rs -> {
                    if (Testing.Print.isEnabled()) connection.print(rs);
                });
                connection.query("SHOW MASTER STATUS", positionAfterUpdate::readFromDatabase);
            }
        }

        //Testing.Print.enable();

        // And consume the one insert ...
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic("myServer.connector_test.products").size()).isEqualTo(2);
        assertThat(records.topics().size()).isEqualTo(1);
        inserts = records.recordsForTopic("myServer.connector_test.products");
        assertInsert(inserts.get(0), "id", 3001);
        assertInsert(inserts.get(1), "id", 3002);

        // Verify that the connector has stopped ...
        completion.await(10, TimeUnit.SECONDS);
        assertThat(completion.hasCompleted()).isTrue();
        assertThat(completion.hasError()).isTrue();
        assertThat(completion.success()).isFalse();
        assertNoRecordsToConsume();
        assertConnectorNotRunning();

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector ...
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();

        // Read the last committed offsets, and verify the binlog coordinates ...
        SourceInfo persistedOffsetSource = new SourceInfo();
        persistedOffsetSource.setServerName(config.getString(MySqlConnectorConfig.SERVER_NAME));
        Map<String, ?> lastCommittedOffset = readLastCommittedOffset(config, persistedOffsetSource.partition());
        persistedOffsetSource.setOffset(lastCommittedOffset);
        Testing.print("Position before inserts: " + positionBeforeInserts);
        Testing.print("Position after inserts:  " + positionAfterInserts);
        Testing.print("Offset: " + lastCommittedOffset);
        Testing.print("Position after update:  " + positionAfterUpdate);
        if (replicaIsMaster) {
            // Same binlog filename ...
            assertThat(persistedOffsetSource.binlogFilename()).isEqualTo(positionBeforeInserts.binlogFilename());
            assertThat(persistedOffsetSource.binlogFilename()).isEqualTo(positionAfterInserts.binlogFilename());
            // Binlog position in offset should be more than before the inserts, but less than the position after the inserts ...
            assertThat(persistedOffsetSource.binlogPosition()).isGreaterThan(positionBeforeInserts.binlogPosition());
            assertThat(persistedOffsetSource.binlogPosition()).isLessThan(positionAfterInserts.binlogPosition());
        } else {
            // the replica is not the same server as the master, so it will have a different binlog filename and position ...
        }
        // Event number is 2 ...
        assertThat(persistedOffsetSource.eventsToSkipUponRestart()).isEqualTo(2);
        // GTID set should match the before-inserts GTID set ...
        // assertThat(persistedOffsetSource.gtidSet()).isEqualTo(positionBeforeInserts.gtidSet());

        Testing.print("*** Restarting connector, and should begin with inserting 3003 (not 109!)");
        start(MySqlConnector.class, config);

        // And consume the insert for 3003 ...
        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        inserts = records.recordsForTopic("myServer.connector_test.products");
        if (inserts == null) {
            updates = records.recordsForTopic("myServer.connector_test.products_on_hand");
            if (updates != null) {
                fail("Restarted connector and missed the insert of product id=3003!");
            }
        }
        // Read the first record produced since we've restarted
        SourceRecord prod3003 = inserts.get(0);
        assertInsert(prod3003, "id", 3003);
        
        // Check that the offset has the correct/expected values ...
        assertOffset(prod3003,"file",lastCommittedOffset.get("file"));
        assertOffset(prod3003,"pos",lastCommittedOffset.get("pos"));
        assertOffset(prod3003,"row",3);
        assertOffset(prod3003,"event",lastCommittedOffset.get("event"));

        // Check that the record has all of the column values ...
        assertValueField(prod3003,"after/id",3003);
        assertValueField(prod3003,"after/name","oak");
        assertValueField(prod3003,"after/description","tree");
        assertValueField(prod3003,"after/weight",987.65d);
        assertValueField(prod3003,"after/volume",0.0d);
        assertValueField(prod3003,"after/alias","oak");
        

        // And make sure we consume that one extra update ...
        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        updates = records.recordsForTopic("myServer.connector_test.products_on_hand");
        assertThat(updates.size()).isEqualTo(1);
        assertUpdate(updates.get(0), "product_id", 109);
        updates.forEach(this::validate);

        // Start the connector again, and we should see the next two
        Testing.print("*** Done with simple insert");

    }

    protected static class BinlogPosition {
        private String binlogFilename;
        private long binlogPosition;
        private String gtidSet;

        public void readFromDatabase(ResultSet rs) throws SQLException {
            if (rs.next()) {
                binlogFilename = rs.getString(1);
                binlogPosition = rs.getLong(2);
                if (rs.getMetaData().getColumnCount() > 4) {
                    // This column exists only in MySQL 5.6.5 or later ...
                    gtidSet = rs.getString(5);// GTID set, may be null, blank, or contain a GTID set
                }
            }
        }

        public String binlogFilename() {
            return binlogFilename;
        }

        public long binlogPosition() {
            return binlogPosition;
        }

        public String gtidSet() {
            return gtidSet;
        }

        public boolean hasGtids() {
            return gtidSet != null;
        }

        @Override
        public String toString() {
            return "file=" + binlogFilename + ", pos=" + binlogPosition + ", gtids=" + (gtidSet != null ? gtidSet : "");
        }
    }

    @Test
    public void shouldConsumeEventsWithNoSnapshot() throws SQLException, InterruptedException {
        Testing.Files.delete(DB_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                              .with(MySqlConnectorConfig.USER, "snapper")
                              .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                              .with(MySqlConnectorConfig.SERVER_ID, 18780)
                              .with(MySqlConnectorConfig.SERVER_NAME, "myServer1")
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "connector_test_ro")
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.name().toLowerCase())
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(9 + 9 + 4 + 5 + 6); // 6 DDL changes
        assertThat(records.recordsForTopic("myServer1.connector_test_ro.products").size()).isEqualTo(9);
        assertThat(records.recordsForTopic("myServer1.connector_test_ro.products_on_hand").size()).isEqualTo(9);
        assertThat(records.recordsForTopic("myServer1.connector_test_ro.customers").size()).isEqualTo(4);
        assertThat(records.recordsForTopic("myServer1.connector_test_ro.orders").size()).isEqualTo(5);
        assertThat(records.topics().size()).isEqualTo(4 + 1);
        assertThat(records.ddlRecordsForDatabase("connector_test_ro").size()).isEqualTo(6);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        records.recordsForTopic("myServer1.connector_test_ro.orders").forEach(record -> {
            print(record);
        });

        records.recordsForTopic("myServer1.connector_test_ro.customers").forEach(record -> {
            print(record);
        });
    }

    @Test
    public void shouldConsumeEventsWithMaskedAndBlacklistedColumns() throws SQLException, InterruptedException {
        Testing.Files.delete(DB_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                              .with(MySqlConnectorConfig.USER, "snapper")
                              .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                              .with(MySqlConnectorConfig.SERVER_ID, 18780)
                              .with(MySqlConnectorConfig.SERVER_NAME, "myServer2")
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "connector_test_ro")
                              .with(MySqlConnectorConfig.COLUMN_BLACKLIST, "connector_test_ro.orders.order_number")
                              .with(MySqlConnectorConfig.MASK_COLUMN(12), "connector_test_ro.customers.email")
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(9 + 9 + 4 + 5);
        assertThat(records.recordsForTopic("myServer2.connector_test_ro.products").size()).isEqualTo(9);
        assertThat(records.recordsForTopic("myServer2.connector_test_ro.products_on_hand").size()).isEqualTo(9);
        assertThat(records.recordsForTopic("myServer2.connector_test_ro.customers").size()).isEqualTo(4);
        assertThat(records.recordsForTopic("myServer2.connector_test_ro.orders").size()).isEqualTo(5);
        assertThat(records.topics().size()).isEqualTo(4);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        // Check that the orders.order_number is not present ...
        records.recordsForTopic("myServer2.connector_test_ro.orders").forEach(record -> {
            print(record);
            Struct value = (Struct) record.value();
            try {
                value.get("order_number");
                fail("The 'order_number' field was found but should not exist");
            } catch (DataException e) {
                // expected
            }
        });

        // Check that the customer.email is masked ...
        records.recordsForTopic("myServer2.connector_test_ro.customers").forEach(record -> {
            Struct value = (Struct) record.value();
            if (value.getStruct("after") != null) {
                assertThat(value.getStruct("after").getString("email")).isEqualTo("************");
            }
            if (value.getStruct("before") != null) {
                assertThat(value.getStruct("before").getString("email")).isEqualTo("************");
            }
            print(record);
        });
    }

}
