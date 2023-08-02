package io.debezium.storage.jdbc;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlTestConnection;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.jdbc.history.JdbcSchemaHistory;
import io.debezium.storage.jdbc.history.JdbcSchemaHistoryConfig;
import io.debezium.storage.jdbc.offset.JdbcOffsetBackingStoreConfig;
import io.debezium.util.Testing;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

public class SqliteOffsetBackingStoreIT extends AbstractOffsetBackingStoreIT {

    private static final String DBNAME = "inventory";
    private static final Integer PORT = 3306;
    private static final String TOPIC_PREFIX = "test";

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("schema-history.db").toAbsolutePath();

    @Before
    public void beforeEach() throws SQLException {
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);

        try (MySqlTestConnection conn = testConnection()) {
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

    protected Configuration.Builder schemaHistory(Configuration.Builder builder) {
        return builder
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + SCHEMA_HISTORY_PATH)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass");
    }

    @Override
    Configuration.Builder config(String jdbcUrl) {
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
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_JDBC_URL.name(), jdbcUrl)
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_USER.name(), "user")
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_PASSWORD.name(), "pass")
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_TABLE_NAME.name(), "offsets_jdbc")
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_TABLE_DDL.name(),
                        "CREATE TABLE %s(id VARCHAR(36) NOT NULL, " +
                                "offset_key VARCHAR(1255), offset_val VARCHAR(1255)," +
                                "record_insert_ts TIMESTAMP NOT NULL," +
                                "record_insert_seq INTEGER NOT NULL" +
                                ")")
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_TABLE_SELECT.name(),
                        "SELECT id, offset_key, offset_val FROM %s " +
                                "ORDER BY record_insert_ts, record_insert_seq")
                .with("offset.flush.interval.ms", "1000")
                .with("offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore");

        return schemaHistory(builder);
    }

    @Override
    String getJdbcUrl() throws IOException {
        File dbFile = File.createTempFile("test-", "db");
        String jdbcUrl = String.format("jdbc:sqlite:%s", dbFile.getAbsolutePath());

        return jdbcUrl;
    }
}
