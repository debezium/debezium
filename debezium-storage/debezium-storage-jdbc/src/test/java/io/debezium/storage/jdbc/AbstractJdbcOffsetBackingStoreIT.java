/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlTestConnection;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.jdbc.history.JdbcSchemaHistory;
import io.debezium.storage.jdbc.history.JdbcSchemaHistoryConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.time.Duration;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

/**
 * Abstract Integration test class for OffsetBackingStore implementations.
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public abstract class AbstractJdbcOffsetBackingStoreIT extends AbstractConnectorTest {
    protected static final String TOPIC_PREFIX = "test";
    private static final String IMAGE = "debezium/example-mysql";
    protected static final Integer PORT = 3306;
    protected static final String ROOT_PASSWORD = "debezium";
    protected static final String USER = "debezium";
    protected static final String PASSWORD = "dbz";
    protected static final String DBNAME = "inventory";

    private static final String PRIVILEGED_USER = "mysqluser";
    private static final String PRIVILEGED_PASSWORD = "mysqlpassword";

    protected static final String TABLE_NAME = "schematest";

    protected static final GenericContainer<?> container = new GenericContainer<>(IMAGE)
            .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
            .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
            .withEnv("MYSQL_USER", PRIVILEGED_USER)
            .withEnv("MYSQL_PASSWORD", PRIVILEGED_PASSWORD)
            .withExposedPorts(PORT)
            .withStartupTimeout(Duration.ofSeconds(180));

    abstract Configuration.Builder config(String jdbcUrl);

    abstract String getJdbcUrl() throws IOException;

    @BeforeClass
    public static void startDatabase() {
        container.start();
    }

    @AfterClass
    public static void stopDatabase() {
        container.stop();
    }

    protected MySqlTestConnection testConnection() {
        final JdbcConfiguration jdbcConfig = JdbcConfiguration.create()
                .withHostname(container.getHost())
                .withPort(container.getMappedPort(PORT))
                .withUser(PRIVILEGED_USER)
                .withPassword(PRIVILEGED_PASSWORD)
                .withDatabase(DBNAME)
                .build();
        return new MySqlTestConnection(jdbcConfig);
    }

    protected Configuration.Builder getBasicConfig() {
        final Configuration.Builder builder = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, container.getHost())
                .with(MySqlConnectorConfig.PORT, container.getMappedPort(PORT))
                .with(MySqlConnectorConfig.USER, USER)
                .with(MySqlConnectorConfig.PASSWORD, PASSWORD)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DBNAME)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DBNAME + "." + TABLE_NAME)
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.SCHEMA_HISTORY, JdbcSchemaHistory.class)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);

        return builder;
    }

    @Test
    public void shouldStartCorrectlyWithJDBCOffsetStorage() throws InterruptedException, IOException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        String jdbcUrl = getJdbcUrl();

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        Configuration config = config(jdbcUrl).build();

        String jdbcUser = config.getString(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name());
        String jdbcPassword = config.getString(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name());

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", TOPIC_PREFIX);

        consumeRecordsByTopic(4);
        validateIfDataIsCreatedInJDBCDatabase(jdbcUrl, jdbcUser, jdbcPassword, "offsets_jdbc");
    }

    /**
     * Function to validate the offset storage data that is created
     * in Database.
     *
     * @param jdbcUrl
     * @param jdbcUser
     * @param jdbcPassword
     */
    private void validateIfDataIsCreatedInJDBCDatabase(String jdbcUrl, String jdbcUser,
                                                       String jdbcPassword, String jdbcTableName) {
        Connection connection = null;
        try {
            // create a database connection
            connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30); // set timeout to 30 sec.

            ResultSet rs = statement.executeQuery(String.format("select * from %s", jdbcTableName));
            while (rs.next()) {
                String offsetKey = rs.getString("offset_key");
                String offsetValue = rs.getString("offset_val");
                String recordInsertTimestamp = rs.getString("record_insert_ts");
                String recordInsertSequence = rs.getString("record_insert_seq");

                Assert.assertFalse(offsetKey.isBlank() && offsetKey.isEmpty());
                Assert.assertFalse(offsetValue.isBlank() && offsetValue.isEmpty());
                Assert.assertFalse(recordInsertTimestamp.isBlank() && recordInsertTimestamp.isEmpty());
                Assert.assertFalse(recordInsertSequence.isBlank() && recordInsertSequence.isEmpty());

            }

        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
