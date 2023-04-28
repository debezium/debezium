/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.connector.mysql.MySqlTestConnection;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.jdbc.offset.JdbcOffsetBackingStoreConfig;
import io.debezium.util.Testing;

public class JdbcSchemaHistoryIT extends AbstractConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("schema-history.db").toAbsolutePath();

    private static final String USER = "debezium";
    private static final String PASSWORD = "dbz";
    private static final String PRIVILEGED_USER = "mysqluser";
    private static final String PRIVILEGED_PASSWORD = "mysqlpassword";
    private static final String ROOT_PASSWORD = "debezium";
    private static final String DBNAME = "inventory";
    private static final String IMAGE = "debezium/example-mysql";
    private static final Integer PORT = 3306;
    private static final String TOPIC_PREFIX = "test";
    private static final String TABLE_NAME = "schematest";

    private static final GenericContainer<?> container = new GenericContainer<>(IMAGE)
            .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
            .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
            .withEnv("MYSQL_USER", PRIVILEGED_USER)
            .withEnv("MYSQL_PASSWORD", PRIVILEGED_PASSWORD)
            .withExposedPorts(PORT)
            .withStartupTimeout(Duration.ofSeconds(180));

    @BeforeClass
    public static void startDatabase() {
        container.start();
    }

    @AfterClass
    public static void stopDatabase() {
        container.stop();
    }

    @Before
    public void beforeEach() throws SQLException {
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);

        try (MySqlTestConnection conn = testConnection()) {
            // System.out.println("DML");
            conn.execute(
                    "DROP TABLE IF EXISTS schematest",
                    "CREATE TABLE schematest (id INT PRIMARY KEY, val VARCHAR(16))",
                    "INSERT INTO schematest VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");
        }

        stopConnector();
    }

    @After
    public void afterEach() throws SQLException {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }

        try (MySqlTestConnection conn = testConnection()) {
            conn.execute("DROP TABLE IF EXISTS schematest");
        }
    }

    private String topicName() {
        return String.format("%s.%s.%s", TOPIC_PREFIX, DBNAME, TABLE_NAME);
    }

    protected Configuration.Builder schemaHistory(Configuration.Builder builder) {
        return builder
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + SCHEMA_HISTORY_PATH)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass");
    }

    private Configuration.Builder config() throws IOException {
        File dbFile = File.createTempFile("test-", "db");

        String jdbcUrl = String.format("jdbc:sqlite:%s", dbFile.getAbsolutePath());

        final Builder builder = Configuration.create()
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
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_JDBC_URL.name(), jdbcUrl)
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_USER.name(), "user")
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_PASSWORD.name(), "pass")
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_TABLE_NAME.name(), "offsets_jdbc");

        return schemaHistory(builder);
    }

    private MySqlTestConnection testConnection() {
        final JdbcConfiguration jdbcConfig = JdbcConfiguration.create()
                .withHostname(container.getHost())
                .withPort(container.getMappedPort(PORT))
                .withUser(PRIVILEGED_USER)
                .withPassword(PRIVILEGED_PASSWORD)
                .withDatabase(DBNAME)
                .build();
        return new MySqlTestConnection(jdbcConfig);
    }

    @Test
    public void shouldStreamChanges() throws InterruptedException, IOException {
        Configuration config = config().build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(4); // 4 DML changes
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic(topicName())).hasSize(4);

        stopConnector();
    }

    @Test
    public void shouldStreamChangesAfterRestart() throws InterruptedException, SQLException, IOException {
        Configuration config = config().build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", TOPIC_PREFIX);

        Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(4); // 4 DML changes
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic(topicName())).hasSize(4);

        try (BufferedReader br = new BufferedReader(new FileReader(String.valueOf(OFFSET_STORE_PATH)))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        stopConnector();

        try (MySqlTestConnection conn = testConnection()) {
            conn.execute("INSERT INTO schematest VALUES (5, 'five')");
        }
        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", TOPIC_PREFIX);

        records = consumeRecordsByTopic(1); // 1 DML change
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic(topicName())).hasSize(1);

        final SourceRecord record = records.recordsForTopic(topicName()).get(0);
        assertThat(((Struct) record.key()).get("id")).isEqualTo(5);
        stopConnector();
    }
}
