/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.zzz;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.connector.mariadb.MariaDbCommon;
import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Verifies how the connector restarts when the binlogs holding its stored GTID position were purged.
 * The test is named to run alphabetically last as purging binlogs can influence other tests.
 */
public class ZZZMariaDbPurgedGtidIT extends AbstractBinlogConnectorIT<MariaDbConnector> implements MariaDbCommon {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-purged-gtid.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", "connector_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @BeforeEach
    void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
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

    @Test
    @FixFor("debezium/dbz#1384")
    public void shouldSnapshotWhenNeededAfterRequiredBinlogsWerePurged() throws Exception {
        final Configuration config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.WHEN_NEEDED)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("customers"))
                .build();

        start(MariaDbConnector.class, config);
        SourceRecords records = consumeRecordsByTopic(4);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("customers"))).hasSize(4);
        stopConnector();

        // Changes written while the connector is down end up in binlogs that are then purged
        insertCustomers(1, 2);
        purgeDatabaseLogs();
        insertCustomers(3, 4);

        // Instead of failing to stream from the purged position, the connector takes a new snapshot
        start(MariaDbConnector.class, config);
        records = consumeRecordsByTopic(8);
        final List<SourceRecord> customers = records.recordsForTopic(DATABASE.topicForTable("customers"));
        assertThat(customers).hasSize(8);
        customers.forEach(record -> {
            final Struct value = (Struct) record.value();
            assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.READ.code());
        });
        records.forEach(this::validate);
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("debezium/dbz#1384")
    public void shouldFailOnStartWhenRequiredBinlogsWerePurgedAndSnapshotNotAllowed() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(ZZZMariaDbPurgedGtidIT.class);
        final Configuration config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("customers"))
                .build();

        start(MariaDbConnector.class, config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        insertCustomers(1);
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("customers"))).hasSize(1);
        stopConnector();

        insertCustomers(2);
        purgeDatabaseLogs();

        start(MariaDbConnector.class, config);
        Awaitility.await().atMost(Duration.ofSeconds(waitTimeForRecords() * 5L))
                .until(() -> logInterceptor.containsErrorMessage("but this is no longer available on the server"));
        assertConnectorNotRunning();
    }

    private void insertCustomers(int... ids) throws SQLException {
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            for (int id : ids) {
                db.execute("INSERT INTO customers VALUES(default,'first" + id + "','last" + id + "','purged" + id + "@acme.com')");
            }
        }
    }

    private void purgeDatabaseLogs() throws SQLException {
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // make sure there's a new log file, then purge all log files other than the last one
                connection.execute("FLUSH LOGS");
                final List<String> binlogs = getBinlogs(connection);
                connection.execute("PURGE BINARY LOGS TO '" + binlogs.get(binlogs.size() - 1) + "'");
                Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> getBinlogs(connection).size() == 1);
            }
        }
    }

    private List<String> getBinlogs(JdbcConnection connection) throws SQLException {
        return connection.queryAndMap("SHOW BINARY LOGS", rs -> {
            final List<String> binlogs = new ArrayList<>();
            while (rs.next()) {
                binlogs.add(rs.getString(1));
            }
            return binlogs;
        });
    }
}
