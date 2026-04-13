/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.kafka.common.config.Config;
import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.BinlogConnectorIT;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.connector.mariadb.MariaDbConnectorConfig.SnapshotLockingMode;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;

/**
 * @author Chris Cranford
 */
public class MariaDbConnectorIT extends BinlogConnectorIT<MariaDbConnector, MariaDbPartition, MariaDbOffsetContext>
        implements MariaDbCommon {

    @Override
    protected Config validateConfiguration(Configuration configuration) {
        return new MariaDbConnector().validate(configuration.asMap());
    }

    @Override
    protected void assertInvalidConfiguration(Config result) {
        super.assertInvalidConfiguration(result);
        assertNoConfigurationErrors(result, MariaDbConnectorConfig.SNAPSHOT_LOCKING_MODE);
        assertNoConfigurationErrors(result, MariaDbConnectorConfig.SSL_MODE);
    }

    @Override
    protected void assertValidConfiguration(Config result) {
        super.assertValidConfiguration(result);
        validateConfigField(result, MariaDbConnectorConfig.SNAPSHOT_LOCKING_MODE, SnapshotLockingMode.MINIMAL);
        validateConfigField(result, MariaDbConnectorConfig.SSL_MODE, MariaDbConnectorConfig.MariaDbSecureConnectionMode.DISABLE);
    }

    @Override
    protected Field getSnapshotLockingModeField() {
        return MariaDbConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }

    @Override
    protected String getSnapshotLockingModeNone() {
        return SnapshotLockingMode.NONE.getValue();
    }

    @Override
    protected void assertSnapshotLockingModeIsNone(Configuration config) {
        assertThat(new MariaDbConnectorConfig(config).getSnapshotLockingMode().get()).isEqualTo(SnapshotLockingMode.NONE);
    }

    @Override
    protected MariaDbPartition createPartition(String serverName, String databaseName) {
        return new MariaDbPartition(serverName, databaseName);
    }

    @Override
    protected MariaDbOffsetContext loadOffsets(Configuration configuration, Map<String, ?> offsets) {
        return new MariaDbOffsetContext.Loader(new MariaDbConnectorConfig(configuration)).load(offsets);
    }

    @Override
    protected void assertBinlogPosition(long offsetPosition, long beforeInsertsPosition) {
        assertThat(offsetPosition).isGreaterThanOrEqualTo(beforeInsertsPosition);
    }

    @Override
    protected String getExpectedQuery(String statement) {

        return "SET STATEMENT max_statement_time=600 FOR " + statement;
    }

    @Test
    @FixFor("debezium/dbz#1378")
    public void shouldStartWithEmptyGtidSet() throws Exception {
        final UniqueDatabase database = getDatabase();

        final Configuration initialConfig = database.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName("products"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .build();

        start(getConnectorClass(), initialConfig);
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        // Generate at least one GTID and persist it into the offset.
        try (BinlogTestConnection db = getTestDatabaseConnection(database.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute("INSERT INTO products VALUES (default,'dbz-1378-prime','Prime',10.50)");
        }

        final SourceRecords firstRunRecords = consumeRecordsByTopic(1);
        assertThat(firstRunRecords.recordsForTopic(database.topicForTable("products")).size()).isEqualTo(1);

        stopConnector();

        // Verify restart offset actually contains GTID state before applying excludes.
        final String serverName = initialConfig.getString(CommonConnectorConfig.TOPIC_PREFIX);
        final Map<String, String> partition = createPartition(serverName, database.getDatabaseName()).getSourcePartition();
        final Map<String, ?> lastCommittedOffset = readLastCommittedOffset(initialConfig, partition);
        final MariaDbOffsetContext offsetContext = loadOffsets(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                .build(), lastCommittedOffset);
        assertThat(offsetContext.gtidSet()).isNotBlank();

        final Configuration restartConfig = database.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName("products"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.GTID_SOURCE_EXCLUDES, ".*")
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .build();

        final Logger binlogClientLogger = Logger.getLogger(BinaryLogClient.class.getName());
        final List<String> secondStartMessages = new ArrayList<>();
        final Handler captureHandler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record != null && record.getLevel().intValue() >= Level.INFO.intValue()) {
                    secondStartMessages.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };

        binlogClientLogger.addHandler(captureHandler);
        try {
            // Re-start with GTID excludes so filtered GTID becomes empty and exercises the problematic path.
            start(getConnectorClass(), restartConfig);
            waitForStreamingRunning(getConnectorName(), database.getServerName());

            try (BinlogTestConnection db = getTestDatabaseConnection(database.getDatabaseName());
                    JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO products VALUES (default,'dbz-1378-restart','Restart',11.50)");
            }

            final SourceRecords secondRunRecords = consumeRecordsByTopic(1);
            assertThat(secondRunRecords.recordsForTopic(database.topicForTable("products")).size()).isEqualTo(1);
            assertConnectorIsRunning();
        }
        finally {
            binlogClientLogger.removeHandler(captureHandler);
        }

        // With the upstream fix (0.40.7+), empty GTID state must not use MariaDB GTID connect-state path.
        // Older behavior (0.40.6) logs this line and follows the buggy path.
        assertThat(secondStartMessages)
                .noneMatch(message -> message != null && message.startsWith("Requesting streaming from GTID set:"));

        stopConnector();
    }
}
