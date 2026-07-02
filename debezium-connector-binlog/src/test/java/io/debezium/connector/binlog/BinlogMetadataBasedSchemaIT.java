/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.util.Testing;

/**
 * Integration tests for the opt-in binlog-metadata-based schema mode (see
 * {@link BinlogConnectorConfig#BINLOG_METADATA_BASED_SCHEMA}), which reconstructs the streaming table
 * schema from the FULL metadata carried by binlog {@code TABLE_MAP} events instead of from a persisted
 * schema history topic (debezium/dbz#978).
 * <p>
 * The mode relies only on the self-describing binlog, so it works for any binlog-based connector whose
 * server can emit {@code binlog_row_metadata=FULL} (MySQL 8.0+, MariaDB 10.5+). The central test,
 * {@link #shouldResumeMidMultiRowInsertWithReconstructedSchema()}, verifies the scenario jpechane asked
 * to focus on: a connector restart in the middle of a multi-row INSERT must resume without losing rows
 * and must decode the resumed rows with the schema rebuilt from the re-read {@code TABLE_MAP} event.
 *
 * @author Debezium contributor
 */
public abstract class BinlogMetadataBasedSchemaIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files
            .createTestingPath("file-schema-history-binlog-metadata.txt").toAbsolutePath();

    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("binlogmeta", "binlog_metadata_schema")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private String originalRowMetadata;

    @BeforeEach
    void beforeEach() throws SQLException {
        stopConnector();
        originalRowMetadata = currentBinlogRowMetadata();
        setBinlogRowMetadata("FULL");
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() throws SQLException {
        try {
            stopConnector();
        }
        finally {
            if (originalRowMetadata != null) {
                setBinlogRowMetadata(originalRowMetadata);
            }
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    public void shouldReconstructSchemaFromBinlogMetadataWithoutHistoryTopic() throws Exception {
        final Configuration config = metadataModeConfig().build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        insertOrders(5);
        final List<SourceRecord> orders = consumeOrders(5);
        assertThat(orders).hasSize(5);

        for (SourceRecord record : orders) {
            assertOrderSchema(afterOf(record));
        }
        // Spot-check a couple of reconstructed values.
        final Struct first = afterOf(orders.get(0));
        assertThat(first.getString("status")).isEqualTo("NEW");
        assertThat(first.get("code")).isNotNull();

        // The mode must not create or depend on a persisted schema history topic/file.
        assertThat(SCHEMA_HISTORY_PATH.toFile().exists()).isFalse();

        stopConnector();
    }

    @Test
    public void shouldSnapshotExistingDataThenStreamWithReconstructedSchema() throws Exception {
        // Rows that exist before the connector starts must be captured by the initial snapshot.
        insertOrders(4);

        final Configuration config = metadataModeConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with("snapshot.locking.mode", "none")
                .build();
        start(getConnectorClass(), config);

        // Snapshot read events (op=r) for the pre-existing rows, decoded with the reconstructed schema.
        final List<SourceRecord> snapshot = consumeOrders(4);
        assertThat(snapshot).hasSize(4);
        for (SourceRecord record : snapshot) {
            assertThat(operationOf(record)).isEqualTo("r");
            assertOrderSchema(afterOf(record));
        }

        // Handoff to streaming: a subsequent change is captured as a create event with the same schema.
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        insertOrders(1);
        final List<SourceRecord> streamed = consumeOrders(1);
        assertThat(streamed).hasSize(1);
        assertThat(operationOf(streamed.get(0))).isEqualTo("c");
        assertOrderSchema(afterOf(streamed.get(0)));

        stopConnector();
    }

    @Test
    public void shouldResumeMidMultiRowInsertWithReconstructedSchema() throws Exception {
        final Configuration config = metadataModeConfig().build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        // A single multi-row INSERT is emitted as one WRITE_ROWS event preceded by one TABLE_MAP event.
        final int total = 20;
        insertOrders(total);

        final Set<Integer> seenIds = new HashSet<>();

        // Consume a prefix of the rows, verify each is decoded with the reconstructed schema, then
        // force a restart. The committed offset lands in the middle of the multi-row WRITE_ROWS event.
        final SourceRecords beforeRestart = consumeRecordsByTopic(8);
        final List<SourceRecord> beforeOrders = beforeRestart.recordsForTopic(DATABASE.topicForTable("orders"));
        assertThat(beforeOrders).isNotEmpty();
        for (SourceRecord record : beforeOrders) {
            assertOrderSchema(afterOf(record));
            seenIds.add(afterOf(record).getInt32("id"));
        }
        stopConnector();

        // Restart: the connector resumes from the mid-event offset. handleTransactionBegin re-reads from
        // the transaction BEGIN, re-reads the TABLE_MAP (rebuilding the schema), skips the already-emitted
        // rows and emits the remainder.
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (seenIds.size() < total && System.currentTimeMillis() < deadline) {
            final SourceRecords more = consumeRecordsByTopic(total);
            final List<SourceRecord> orders = more.recordsForTopic(DATABASE.topicForTable("orders"));
            if (orders == null) {
                continue;
            }
            for (SourceRecord record : orders) {
                // The key assertion: resumed rows still decode with the full schema reconstructed from
                // the re-read TABLE_MAP event.
                assertOrderSchema(afterOf(record));
                seenIds.add(afterOf(record).getInt32("id"));
            }
        }

        // No rows may be lost across the mid-event restart (at-least-once duplicates are tolerated).
        assertThat(seenIds).hasSize(total);
        assertThat(SCHEMA_HISTORY_PATH.toFile().exists()).isFalse();

        stopConnector();
    }

    @Test
    public void shouldFailFastWhenBinlogRowMetadataIsNotFull() throws Exception {
        setBinlogRowMetadata("MINIMAL");

        final Configuration config = metadataModeConfig().build();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, throwable) -> {
            if (!success) {
                error.set(throwable);
            }
        });

        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (error.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }

        assertThat(error.get())
                .as("connector should fail fast when binlog_row_metadata is not FULL")
                .isNotNull();
        assertThat(error.get().getMessage()).contains("binlog_row_metadata");
    }

    @Test
    public void shouldStreamFromCurrentPositionWhenSnapshotIsSkipped() throws Exception {
        // Rows that already exist before the connector starts. With a no_data snapshot in this
        // (non-historized) mode the snapshot is skipped, so the connector must seed the current binlog
        // position and must NOT replay these pre-existing rows.
        insertOrders(3);

        final Configuration config = metadataModeConfig().build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        // A change made after streaming has started must be captured.
        insertMarkerOrder("MK00");

        // It must be the FIRST record we see: if streaming had begun from the earliest binlog, the
        // pre-existing rows would have been emitted first.
        final List<SourceRecord> orders = consumeOrders(1);
        assertThat(orders).hasSize(1);
        final Struct after = afterOf(orders.get(0));
        assertThat(operationOf(orders.get(0))).isEqualTo("c");
        assertThat(after.getString("code")).isEqualTo("MK00");
        assertOrderSchema(after);

        stopConnector();
    }

    private Configuration.Builder metadataModeConfig() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.BINLOG_METADATA_BASED_SCHEMA, true);
    }

    /**
     * Consume order change records until {@code expected} of them have been collected or a timeout elapses.
     * This tolerates the streaming latency of a freshly started connector.
     */
    private List<SourceRecord> consumeOrders(int expected) throws InterruptedException {
        final List<SourceRecord> collected = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (collected.size() < expected && System.currentTimeMillis() < deadline) {
            final SourceRecords records = consumeRecordsByTopic(expected - collected.size());
            final List<SourceRecord> orders = records.recordsForTopic(DATABASE.topicForTable("orders"));
            if (orders != null) {
                collected.addAll(orders);
            }
        }
        return collected;
    }

    private void insertOrders(int count) throws SQLException {
        final StringBuilder sql = new StringBuilder(
                "INSERT INTO orders (quantity, status, price, code, note, created_at) VALUES ");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append(String.format("(%d,'NEW',%d.500,'C%03d','note-%d',NOW(3))", 100 + i, 10 + i, i, i));
        }
        executeStatements(DATABASE.getDatabaseName(), sql.toString());
    }

    private void insertMarkerOrder(String code) throws SQLException {
        executeStatements(DATABASE.getDatabaseName(), String.format(
                "INSERT INTO orders (quantity, status, price, code, note, created_at) "
                        + "VALUES (1, 'NEW', 1.000, '%s', 'marker', NOW(3))",
                code));
    }

    private static Struct afterOf(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private static String operationOf(SourceRecord record) {
        return ((Struct) record.value()).getString(Envelope.FieldName.OPERATION);
    }

    private static void assertOrderSchema(Struct after) {
        final Schema schema = after.schema();
        assertThat(schema.field("id")).isNotNull();
        assertThat(schema.field("quantity")).isNotNull();
        assertThat(schema.field("status")).isNotNull();
        assertThat(schema.field("price")).isNotNull();
        assertThat(schema.field("code")).isNotNull();
        assertThat(schema.field("note")).isNotNull();
        assertThat(schema.field("created_at")).isNotNull();
        // Types reconstructed from TABLE_MAP metadata.
        assertThat(schema.field("id").schema().type()).isEqualTo(Schema.Type.INT32);
        assertThat(schema.field("code").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(schema.field("quantity").schema().type()).isEqualTo(Schema.Type.INT64);
    }

    private String currentBinlogRowMetadata() throws SQLException {
        try (BinlogTestConnection db = getTestDatabaseConnection("mysql")) {
            return db.queryAndMap("SHOW GLOBAL VARIABLES LIKE 'binlog_row_metadata'",
                    rs -> rs.next() ? rs.getString(2) : null);
        }
    }

    private void setBinlogRowMetadata(String value) throws SQLException {
        try (BinlogTestConnection db = getTestDatabaseConnection("mysql")) {
            db.execute("SET GLOBAL binlog_row_metadata = '" + value + "'");
        }
    }
}
