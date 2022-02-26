/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

public class MysqlConnectorSslIT extends AbstractConnectorTest {
    private static final String TABLE_NAME = "dbz4787";
    private static final String TRUST_STORE_PATH = "ssl/truststore";
    private static final String TRUST_STORE_PASSWORD = "debezium";
    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-decimal.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("ssldb", "ssl_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
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
    @FixFor("DBZ-4787")
    public void testSslRequireModeWithNullStore() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.SecureConnectionMode.REQUIRED)
                .build();

        start(MySqlConnector.class, config);

        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());
        insertOneRecord();

        assertChangeRecord(consumeInsert());

        stopConnector();
    }

    @Test
    @FixFor("DBZ-4787")
    public void testSslRequireMode() throws SQLException, InterruptedException {
        final URL trustStoreFile = MysqlConnectorSslIT.class.getClassLoader().getResource(TRUST_STORE_PATH);

        if (trustStoreFile != null) {
            config = DATABASE.defaultConfig()
                    .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                    .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                    .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.SecureConnectionMode.REQUIRED)
                    .with(MySqlConnectorConfig.SSL_TRUSTSTORE, trustStoreFile.getPath())
                    .with(MySqlConnectorConfig.SSL_TRUSTSTORE_PASSWORD, TRUST_STORE_PASSWORD)
                    .build();

            start(MySqlConnector.class, config);

            waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());
            insertOneRecord();

            assertChangeRecord(consumeInsert());

            stopConnector();
        }
    }

    @Test
    @FixFor("DBZ-4787")
    public void testSslVerifyCaMode() throws SQLException, InterruptedException {
        final URL trustStoreFile = MysqlConnectorSslIT.class.getClassLoader().getResource(TRUST_STORE_PATH);

        if (trustStoreFile != null) {
            config = DATABASE.defaultConfig()
                    .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                    .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                    .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.SecureConnectionMode.VERIFY_CA)
                    .with(MySqlConnectorConfig.SSL_KEYSTORE, trustStoreFile.getPath())
                    .with(MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD, TRUST_STORE_PASSWORD)
                    .with(MySqlConnectorConfig.SSL_TRUSTSTORE, trustStoreFile.getPath())
                    .with(MySqlConnectorConfig.SSL_TRUSTSTORE_PASSWORD, TRUST_STORE_PASSWORD)
                    .build();

            start(MySqlConnector.class, config);

            waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());
            insertOneRecord();

            assertChangeRecord(consumeInsert());

            stopConnector();
        }
    }

    private SourceRecord consumeInsert() throws InterruptedException {
        final int numDatabase = 2;
        final int numTables = 4;
        final int numOthers = 1;

        SourceRecords records = consumeRecordsByTopic(numDatabase + numTables + numOthers);

        assertThat(records).isNotNull();

        List<SourceRecord> events = records.recordsForTopic(DATABASE.topicForTable(TABLE_NAME));
        assertThat(events).hasSize(1);

        return events.get(0);
    }

    private void insertOneRecord() throws SQLException {
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO `dbz4787`(a, b, c) VALUES ('enable ssl mode', 234.123, 100);");
            }
        }
    }

    private void assertChangeRecord(SourceRecord record) {
        assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        assertThat(change.getString("a").trim()).isEqualTo("enable ssl mode");
        assertThat(change.get("b")).isEqualTo(new BigDecimal("234.1230000000"));
        assertThat(change.getInt32("c")).isEqualTo(100);
    }
}
