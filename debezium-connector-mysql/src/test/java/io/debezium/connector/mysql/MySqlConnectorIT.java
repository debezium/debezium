/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorIT;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotLockingMode;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorIT extends BinlogConnectorIT<MySqlConnector, MySqlPartition, MySqlOffsetContext> implements MySqlCommon {

    @Test
    void shouldNotStartWithUnknownJdbcDriver() {
        final Configuration config = getDatabase().defaultConfig()
                .with(MySqlConnectorConfig.JDBC_DRIVER, "foo.bar")
                .build();

        final AtomicBoolean successResult = new AtomicBoolean();
        final AtomicReference<String> message = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, msg, error) -> {
            successResult.set(success);
            message.set(msg);
        });

        assertThat(successResult.get()).isEqualTo(false);
        assertThat(message.get()).contains("java.lang.ClassNotFoundException: foo.bar");
        assertConnectorNotRunning();
    }

    @Test
    void shouldNotStartWithWrongProtocol() {
        final Configuration config = getDatabase().defaultConfig()
                .with(MySqlConnectorConfig.JDBC_PROTOCOL, "foo:bar")
                .build();

        final AtomicBoolean successResult = new AtomicBoolean();
        final AtomicReference<String> message = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, msg, error) -> {
            successResult.set(success);
            message.set(msg);
        });

        assertThat(successResult.get()).isEqualTo(false);
        assertThat(message.get()).contains("Unable to obtain a JDBC connection");
        assertConnectorNotRunning();
    }

    @Test
    @FixFor("debezium/dbz#1439")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 8, reason = "Instant ADD COLUMN requires MySQL 8.0+")
    void shouldStreamMultipleAddColumnsWithRepeatedInstantAlgorithm() throws Exception {
        final String tableName = "test_lot";

        try (BinlogTestConnection db = getTestDatabaseConnection(getDatabase().getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute("CREATE TABLE `test_lot` ("
                    + "`lot_id` bigint unsigned NOT NULL,"
                    + "`trade_date` date NOT NULL,"
                    + "PRIMARY KEY (`lot_id`,`trade_date`)"
                    + ") ENGINE=InnoDB");
        }

        final Configuration config = getDatabase().defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, getDatabase().qualifiedTableName(tableName))
                .build();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), getDatabase().getServerName(), getStreamingNamespace());

        try (BinlogTestConnection db = getTestDatabaseConnection(getDatabase().getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute("ALTER TABLE test_lot ADD COLUMN event_ref_type_id INTEGER, algorithm=instant, "
                    + "ADD COLUMN event_ref_id BIGINT, algorithm=instant");
            connection.execute("INSERT INTO test_lot VALUES (1, '2026-07-07', 2, 3)");
        }

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> tableRecords = records.recordsForTopic(getDatabase().topicForTable(tableName));
        assertThat(tableRecords).hasSize(1);

        final SourceRecord record = tableRecords.get(0);
        validate(record);

        final Struct after = ((Struct) record.value()).getStruct("after");
        assertThat(after.schema().field("event_ref_type_id")).isNotNull();
        assertThat(after.schema().field("event_ref_id")).isNotNull();
        assertThat(after.getInt32("event_ref_type_id")).isEqualTo(2);
        assertThat(after.getInt64("event_ref_id")).isEqualTo(3L);
    }

    @Override
    protected Config validateConfiguration(Configuration configuration) {
        return new MySqlConnector().validate(configuration.asMap());
    }

    @Override
    protected void assertInvalidConfiguration(Config result) {
        super.assertInvalidConfiguration(result);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SSL_MODE);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.IGNORE_GTID_ON_RECOVERY);
    }

    @Override
    protected void assertValidConfiguration(Config result) {
        super.assertValidConfiguration(result);
        validateConfigField(result, MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, SnapshotLockingMode.MINIMAL);
        validateConfigField(result, MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.MySqlSecureConnectionMode.PREFERRED);
        validateConfigField(result, MySqlConnectorConfig.IGNORE_GTID_ON_RECOVERY, false);
    }

    @Override
    protected Field getSnapshotLockingModeField() {
        return MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }

    @Override
    protected String getSnapshotLockingModeNone() {
        return SnapshotLockingMode.NONE.getValue();
    }

    @Override
    protected void assertSnapshotLockingModeIsNone(Configuration config) {
        assertThat(new MySqlConnectorConfig(config).getSnapshotLockingMode().get()).isEqualTo(SnapshotLockingMode.NONE);
    }

    @Override
    protected MySqlPartition createPartition(String serverName, String databaseName) {
        return new MySqlPartition(serverName, databaseName);
    }

    @Override
    protected MySqlOffsetContext loadOffsets(Configuration configuration, Map<String, ?> offsets) {
        return new MySqlOffsetContext.Loader(new MySqlConnectorConfig(configuration)).load(offsets);
    }

    @Override
    protected void assertBinlogPosition(long offsetPosition, long beforeInsertsPosition) {
        assertThat(offsetPosition).isGreaterThan(beforeInsertsPosition);
    }
}
