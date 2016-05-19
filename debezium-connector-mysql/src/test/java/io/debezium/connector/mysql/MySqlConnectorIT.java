/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history.txt").toAbsolutePath();

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        stopConnector();
        Testing.Files.delete(DB_HISTORY_PATH);
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
    public void shouldStartAndPollShouldReturnSourceRecordsFromDatabase() throws SQLException {
        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.query("SELECT * FROM products", rs->{if (Testing.Print.isEnabled()) connection.print(rs);});
                connection.execute("INSERT INTO products VALUES (default,'robot','Toy robot',1.304);");
                connection.query("SELECT * FROM products", rs->{if (Testing.Print.isEnabled()) connection.print(rs);});
            }
        }

        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                              .with(MySqlConnectorConfig.USER, "replicator")
                              .with(MySqlConnectorConfig.PASSWORD, "replpass")
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, "kafka-connect")
                              .with(MySqlConnectorConfig.INITIAL_BINLOG_FILENAME, "mysql-bin.000001")
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "connector_test")
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        waitForAvailableRecords(10, TimeUnit.SECONDS);

        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO products VALUES (default,'harrison','real robot',134.82);");
                connection.query("SELECT * FROM products", rs->{if (Testing.Print.isEnabled()) connection.print(rs);});
            }
        }
        
        //Testing.Print.enable();
        int totalConsumed = consumeAvailableRecords(this::print);  // expecting at least 1
        stopConnector();
        
        // Restart the connector and wait for a few seconds (at most) for records that will never arrive ...
        start(MySqlConnector.class, config);
        waitForAvailableRecords(2, TimeUnit.SECONDS);
        totalConsumed += consumeAvailableRecords(this::print);
        stopConnector();
        
        // Create an additional few records ...
        Testing.Print.disable();
        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO products VALUES (1001,'roy','old robot',1234.56);");
                connection.query("SELECT * FROM products", rs->{if (Testing.Print.isEnabled()) connection.print(rs);});
            }
        }

        // Restart the connector and wait for a few seconds (at most) for the new record ...
        //Testing.Print.enable();
        start(MySqlConnector.class, config);
        waitForAvailableRecords(5, TimeUnit.SECONDS);
        totalConsumed += consumeAvailableRecords(this::print);

        try (MySQLConnection db = MySQLConnection.forTestDatabase("connector_test");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE products SET id=2001, description='really old robot' WHERE id=1001");
                connection.query("SELECT * FROM products", rs->{if (Testing.Print.isEnabled()) connection.print(rs);});
            }
        }
        waitForAvailableRecords(5, TimeUnit.SECONDS);
        List<SourceRecord> deletes = new ArrayList<>();
        totalConsumed += consumeAvailableRecords(deletes::add);
        stopConnector();
        
        // Verify that the update of a record where the pk changes results in
        // 1 update, 1 delete, and 1 tombstone event ...
        assertThat(deletes.size()).isEqualTo(3);
        assertInsert(deletes.get(0),"id",2001);
        assertDelete(deletes.get(1),"id",1001);
        assertTombstone(deletes.get(2),"id",1001);

        // We should have seen a total of 33 events, though when they appear may vary ...
        assertThat(totalConsumed).isEqualTo(33);
    }
    
    @Test
    public void shouldConsumeEventsWithMaskedAndBlacklistedColumns() throws SQLException {
        Testing.Files.delete(DB_HISTORY_PATH);
        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                              .with(MySqlConnectorConfig.USER, "replicator")
                              .with(MySqlConnectorConfig.PASSWORD, "replpass")
                              .with(MySqlConnectorConfig.SERVER_ID, 18780)
                              .with(MySqlConnectorConfig.SERVER_NAME, "kafka-connect-2")
                              .with(MySqlConnectorConfig.INITIAL_BINLOG_FILENAME, "mysql-bin.000001")
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "connector_test")
                              .with(MySqlConnectorConfig.COLUMN_BLACKLIST, "connector_test.orders.order_number")
                              .with(MySqlConnectorConfig.MASK_COLUMN(12), "connector_test.customers.email")
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // Wait for records to become available ...
        //Testing.Print.enable();
        waitForAvailableRecords(15, TimeUnit.SECONDS);
        
        // Now consume the records ...
        int totalConsumed = consumeAvailableRecords((record)->{
            print(record);
            if ( record.topic().endsWith(".orders")) {
                Struct value = (Struct) record.value();
                try {
                    value.get("order_number");
                    fail("The 'order_number' field was found but should not exist");
                } catch ( DataException e ) {
                    // expected
                    printJson(record);
                }
            } else if ( record.topic().endsWith(".customers")) {
                Struct value = (Struct) record.value();
                if ( value.getStruct("after") != null ) {
                    assertThat(value.getStruct("after").getString("email")).isEqualTo("************");
                }
                if ( value.getStruct("before") != null ) {
                    assertThat(value.getStruct("before").getString("email")).isEqualTo("************");
                }
                printJson(record);
            }
        });
        stopConnector();

        // We should have seen a total of 27 events, though when they appear may vary ...
        assertThat(totalConsumed).isEqualTo(27);
    }
    
}
