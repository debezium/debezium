/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.connector.sqlserver.util.TestHelper.SCHEMA_HISTORY_PATH;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.engine.StopEngineException;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.util.Testing;

public class SqlServerSchemaHistoryRecoveryIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;

    @BeforeEach
    void before() throws SQLException, InterruptedException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute("CREATE TABLE orders (id int primary key, status varchar(30))");
        TestHelper.enableTableCdc(connection, "orders");
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        Thread.sleep(Duration.ofSeconds(TestHelper.waitTimeForLsnTimeMapping()).toMillis());
    }

    @AfterEach
    void after() throws SQLException {
        stopConnector();
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#2867")
    void restartAfterMigratedRowRecoversSchemaChanges() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.orders")
                .with(Heartbeat.HEARTBEAT_INTERVAL, 200)
                .with(CommonConnectorConfig.POLL_INTERVAL_MS, 200)
                .with(SqlServerConnectorConfig.MAX_TRANSACTIONS_PER_ITERATION, 0)
                .build();

        final AtomicReference<Lsn> captureStart = new AtomicReference<>();
        start(SqlServerConnector.class, config, loggingCompletion(), null, stopOnMigratedRow(captureStart), true);
        assertConnectorIsRunning();
        TestHelper.waitForStreamingStarted();
        consumedLines.clear();

        replaceCaptureInstance();
        Awaitility.await("new capture instance")
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(Duration.ofSeconds(TestHelper.waitTimeForLogEntries()))
                .until(() -> {
                    consumedLines.clear();
                    return rememberCaptureStart(captureStart);
                });
        connection.execute("INSERT INTO orders VALUES(2, 'paid', 20)");
        Awaitility.await("migrated row committed")
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(Duration.ofSeconds(TestHelper.waitTimeForLogEntries() * 4L))
                .until(() -> {
                    consumedLines.clear();
                    return !isEngineRunning.get();
                });
        waitForEngineShutdown();
        cleanupTestFwkState();

        connection.execute("INSERT INTO orders VALUES(3, 'shipped', 30)");
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        final SourceRecord recovered = awaitRecordWithAmount();
        assertThat(after(recovered).getInt32("id")).isEqualTo(3);
    }

    private Consumer<SourceRecord> stopOnMigratedRow(AtomicReference<Lsn> captureStart) {
        return record -> {
            final Lsn start = captureStart.get();
            if (start == null || !migrationRecorded() || !(record.value() instanceof Struct value)) {
                return;
            }
            if (value.schema().name() == null || !value.schema().name().endsWith(".Envelope")) {
                return;
            }
            final Struct row = value.getStruct("after");
            final Object commitLsn = record.sourceOffset().get(SourceInfo.COMMIT_LSN_KEY);
            if (row == null || row.schema().field("amount") == null || commitLsn == null) {
                return;
            }
            if (start.compareTo(Lsn.valueOf(commitLsn.toString())) <= 0) {
                throw new StopEngineException("Migrated row stored");
            }
        };
    }

    private void replaceCaptureInstance() throws SQLException {
        connection.execute("ALTER TABLE orders ADD amount int");
        TestHelper.disableTableCdc(connection, "orders");
        TestHelper.enableTableCdc(connection, "orders", "orders_amount");
    }

    private boolean rememberCaptureStart(AtomicReference<Lsn> captureStart) throws SQLException {
        final Lsn start = captureStartLsn();
        if (start == null) {
            return false;
        }
        captureStart.set(start);
        return true;
    }

    private SourceRecord awaitRecordWithAmount() {
        return Awaitility.await("orders record with amount")
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(Duration.ofSeconds(TestHelper.waitTimeForLogEntries() * 4L))
                .until(this::nextRecordWithAmount, record -> record != null);
    }

    private SourceRecord nextRecordWithAmount() throws InterruptedException {
        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> topicRecords = records.recordsForTopic("server1.testDB1.dbo.orders");
        if (topicRecords == null) {
            return null;
        }
        return topicRecords.stream()
                .filter(record -> record.value() instanceof Struct)
                .filter(record -> {
                    final Struct after = ((Struct) record.value()).getStruct("after");
                    return after != null && after.schema().field("amount") != null;
                })
                .findFirst()
                .orElse(null);
    }

    private static boolean migrationRecorded() {
        try {
            return java.nio.file.Files.readString(SCHEMA_HISTORY_PATH).contains("\"amount\"");
        }
        catch (IOException e) {
            return false;
        }
    }

    private Lsn captureStartLsn() throws SQLException {
        return connection.queryAndMap(
                "SELECT start_lsn FROM cdc.change_tables WHERE capture_instance = 'orders_amount'",
                rs -> rs.next() ? Lsn.valueOf(rs.getBytes(1)) : null);
    }

    private static Struct after(SourceRecord record) {
        final Struct value = (Struct) record.value();
        final Field after = value.schema().field("after");
        assertThat(after).isNotNull();
        return value.getStruct("after");
    }
}
