/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlTestConnection;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.jdbc.history.JdbcSchemaHistoryConfig;
import io.debezium.storage.jdbc.offset.JdbcOffsetBackingStoreConfig;
import io.debezium.testing.testcontainers.util.ContainerImageVersions;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.sql.SQLException;

/**
 * Integration test for JdbcOffsetBackingStore using MySQL.
 */
public class MySqlOffsetBackingStoreIT extends AbstractOffsetBackingStoreIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlOffsetBackingStoreIT.class);

    private MySQLContainer mySQLContainer;

    private final static String MYSQL_OFFSET_DATABASE_NAME = "offsets";
    private final static String MYSQL_OFFSET_USER = "mysqluser";
    private final static String MYSQL_OFFSET_PASSWORD = "mysql";

    private final static String MYSQL_OFFSET_TABLE_NAME = "offsets_jdbc";

    private void startJDBCOffsetMySQLContainer() {
        String mysqlImage = ContainerImageVersions.getStableImage("quay.io/debezium/example-mysql");
        DockerImageName mysqlDockerImageName = DockerImageName.parse(mysqlImage).asCompatibleSubstituteFor("mysql");
        mySQLContainer = new MySQLContainer<>(mysqlDockerImageName)
                .withDatabaseName(MYSQL_OFFSET_DATABASE_NAME)
                .withUsername(MYSQL_OFFSET_USER)
                .withPassword(MYSQL_OFFSET_PASSWORD)
                .withClasspathResourceMapping("/docker/conf/mysql.cnf", "/etc/mysql/conf.d/", BindMode.READ_ONLY)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withExposedPorts(3306)
                .withNetworkAliases("mysql");
        mySQLContainer.start();
    }

    @Before
    public void beforeEach() throws SQLException {
        initializeConnectorTestFramework();

        try (MySqlTestConnection conn = testConnection()) {
            conn.execute(
                    "DROP TABLE IF EXISTS schematest",
                    "CREATE TABLE schematest (id INT PRIMARY KEY, val VARCHAR(16))",
                    "INSERT INTO schematest VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");
        }

        stopConnector();

        startJDBCOffsetMySQLContainer();
    }

    @After
    public void afterEach() throws SQLException {
        stopConnector();

        try (MySqlTestConnection conn = testConnection()) {
            conn.execute("DROP TABLE IF EXISTS schematest");
        }
        mySQLContainer.stop();
    }

    @Override
    protected Configuration.Builder config(String jdbcUrl) {
        Configuration.Builder builder = getBasicConfig()
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_JDBC_URL.name(), jdbcUrl)
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_USER.name(), MYSQL_OFFSET_USER)
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_PASSWORD.name(), MYSQL_OFFSET_PASSWORD)
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_TABLE_NAME.name(), MYSQL_OFFSET_TABLE_NAME)
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

    protected Configuration.Builder schemaHistory(Configuration.Builder builder) {
        return builder
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), getJdbcUrl())
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), MYSQL_OFFSET_USER)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), MYSQL_OFFSET_PASSWORD)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_TABLE_DDL.name(),
                        "CREATE TABLE %s" + "(" +
                                "id VARCHAR(36) NOT NULL," +
                                "history_data TEXT," +
                                "history_data_seq INTEGER," +
                                "record_insert_ts TIMESTAMP NOT NULL," +
                                "record_insert_seq INTEGER NOT NULL" +
                                ")");
    }

    @Override
    protected String getJdbcUrl() {
        return mySQLContainer.getJdbcUrl();
    }
}
