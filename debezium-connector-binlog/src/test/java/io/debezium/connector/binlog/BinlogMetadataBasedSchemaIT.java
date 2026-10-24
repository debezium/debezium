/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.relational.mapping.PropagateSourceMetadataToSchemaParameter;
import io.debezium.util.Testing;

/**
 * Integration tests for the opt-in binlog-metadata-based schema mode (see
 * {@link BinlogConnectorConfig#BINLOG_METADATA_BASED_SCHEMA}), which reconstructs the streaming table
 * schema from the FULL metadata carried by binlog {@code TABLE_MAP} events instead of from a persisted
 * schema history topic (debezium/dbz#978).
 * <p>
 * The mode relies only on the self-describing binlog, so it works for any binlog-based connector whose
 * server can emit {@code binlog_row_metadata=FULL} (MySQL 8.0+, MariaDB 10.5+). The central test,
 * {@link #shouldResumeMidMultiRowInsertWithReconstructedSchema()}, covers the key scenario: a connector
 * restart in the middle of a multi-row INSERT must resume without losing rows and must decode the
 * resumed rows with the schema rebuilt from the re-read {@code TABLE_MAP} event.
 */
public abstract class BinlogMetadataBasedSchemaIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files
            .createTestingPath("file-schema-history-binlog-metadata.txt").toAbsolutePath();

    protected final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("binlogmeta", "binlog_metadata_schema")
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
    public void shouldReconstructAllDataTypesFromBinlogMetadata() throws Exception {
        final Configuration config = metadataModeConfig()
                .with(BinlogConnectorConfig.PROPAGATE_COLUMN_SOURCE_TYPE, ".*")
                .build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        insertAllTypesRow();

        final List<SourceRecord> records = consumeTable("all_types", 1);
        assertThat(records).hasSize(1);
        final SourceRecord record = records.get(0);
        // A successful decode of a fully-populated row proves every column type code was mapped to a
        // schema the value converters accept.
        assertThat(operationOf(record)).isEqualTo("c");
        final Struct after = afterOf(record);

        // Every column must be present in the schema reconstructed from the TABLE_MAP metadata.
        for (String column : ALL_TYPES_COLUMNS) {
            assertThat(after.schema().field(column)).as("column '%s' reconstructed", column).isNotNull();
        }

        // Spot-check values whose representation is identical on MySQL and MariaDB.
        assertThat(after.getInt32("id")).isEqualTo(1);
        assertThat(after.getInt32("c_int")).isEqualTo(42);
        assertThat(after.getInt64("c_bigint")).isEqualTo(9_000_000_000L);
        assertThat(after.getString("c_char")).isEqualTo("ABCD");
        assertThat(after.getString("c_varchar")).isEqualTo("hello");
        assertThat(after.getString("c_enum")).isEqualTo("OK");
        assertThat(after.getString("c_set")).isEqualTo("a,c");

        assertSourceColumnType(after, "c_tinyblob", "TINYBLOB");
        assertSourceColumnType(after, "c_blob", "BLOB");
        assertSourceColumnType(after, "c_mediumblob", "MEDIUMBLOB");
        assertSourceColumnType(after, "c_longblob", "LONGBLOB");
        assertSourceColumnType(after, "c_tinytext", "TINYTEXT");
        assertSourceColumnType(after, "c_text", "TEXT");
        assertSourceColumnType(after, "c_mediumtext", "MEDIUMTEXT");
        assertSourceColumnType(after, "c_longtext", "LONGTEXT");

        stopConnector();
    }

    @Test
    public void shouldPreserveSignednessWhenNumericAndCharacterColumnsAreInterleaved() throws Exception {
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE numbers (id INT PRIMARY KEY, firstText VARCHAR(10), "
                        + "bigUnsigned BIGINT UNSIGNED, signedInt INT, secondText VARCHAR(10), unsignedInt INT UNSIGNED)");
        final Configuration config = metadataModeConfig()
                .with(BinlogConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE, BinlogConnectorConfig.BigIntUnsignedHandlingMode.PRECISE)
                .build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO numbers VALUES (1, 'first', 18446744073709551615, -42, 'second', 4294967295)");
        final List<SourceRecord> records = consumeTable("numbers", 1);
        assertThat(records).hasSize(1);
        final Struct after = afterOf(records.get(0));
        assertThat(after.get("bigUnsigned")).isEqualTo(new BigDecimal("18446744073709551615"));
        assertThat(after.getInt32("signedInt")).isEqualTo(-42);
        assertThat(after.getInt64("unsignedInt")).isEqualTo(4294967295L);

        stopConnector();
    }

    @Test
    public void shouldRefreshSchemaWhenTableIsAltered() throws Exception {
        final Configuration config = metadataModeConfig().build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        // First write: schema before the change (no 'priority' column).
        insertOrders(1);
        final List<SourceRecord> before = consumeOrders(1);
        assertThat(before).hasSize(1);
        assertThat(afterOf(before.get(0)).schema().field("priority")).isNull();

        // Schema change mid-stream: the parsed DDL marks the table dirty, so the next TABLE_MAP (which
        // carries the new column) must rebuild the schema rather than reuse the registered one.
        executeStatements(DATABASE.getDatabaseName(), "ALTER TABLE orders ADD COLUMN priority INT");
        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO orders (quantity, status, price, code, note, created_at, priority) "
                        + "VALUES (1, 'NEW', 1.000, 'ALT0', 'altered', NOW(3), 7)");

        final List<SourceRecord> after = consumeOrders(1);
        assertThat(after).hasSize(1);
        final Struct struct = afterOf(after.get(0));
        assertThat(struct.schema().field("priority")).as("schema rebuilt after ALTER").isNotNull();
        assertThat(struct.getInt32("priority")).isEqualTo(7);

        stopConnector();
    }

    @Test
    public void shouldSnapshotExistingDataThenStreamWithReconstructedSchema() throws Exception {
        // Rows that exist before the connector starts must be captured by the initial snapshot.
        insertOrders(4);

        final Configuration config = metadataModeConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();
        start(getConnectorClass(), config);

        // Snapshot read events (op=r) use the table schema obtained through the existing DDL path.
        final List<SourceRecord> snapshot = consumeOrders(4);
        assertThat(snapshot).hasSize(4);
        for (SourceRecord record : snapshot) {
            assertThat(operationOf(record)).isEqualTo("r");
            assertOrderSchema(afterOf(record));
        }

        // The first streaming change uses TABLE_MAP metadata. Check the operation and selected fields;
        // the full schema can differ from the snapshot schema, for example in column defaults.
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        insertOrders(1);
        final List<SourceRecord> streamed = consumeOrders(1);
        assertThat(streamed).hasSize(1);
        assertThat(operationOf(streamed.get(0))).isEqualTo("c");
        assertOrderSchema(afterOf(streamed.get(0)));

        stopConnector();
    }

    @ParameterizedTest
    @ValueSource(strings = { "candidate INT", "candidate INT UNIQUE" })
    public void shouldRejectSnapshotWithoutRecoverableKey(String keyDefinition) throws Exception {
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE keytest (" + keyDefinition + ", payload VARCHAR(10))",
                "INSERT INTO keytest VALUES (1, 'snapshot')");

        final AtomicReference<Throwable> error = startWithErrorCapture(keyTestConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL).build());

        assertKeyValidationFailure(error);
        assertNoRecordsToConsume();
    }

    @ParameterizedTest
    @ValueSource(strings = { "other:candidate", "keytest:missing" })
    public void shouldRejectSnapshotWithoutMatchingKeyConfiguration(String keyMapping) throws Exception {
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE keytest (candidate INT UNIQUE, payload VARCHAR(10))",
                "INSERT INTO keytest VALUES (1, 'snapshot')");

        final AtomicReference<Throwable> error = startWithErrorCapture(keyTestConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.MSG_KEY_COLUMNS, DATABASE.getDatabaseName() + "." + keyMapping).build());

        assertKeyValidationFailure(error);
        assertNoRecordsToConsume();
    }

    @ParameterizedTest
    @ValueSource(strings = { "candidate INT", "candidate INT UNIQUE", "candidate INT NOT NULL UNIQUE" })
    public void shouldPreserveKeyAcrossSnapshotStreamingAndRestart(String keyDefinition) throws Exception {
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE keytest (" + keyDefinition + ", payload VARCHAR(10))",
                "INSERT INTO keytest VALUES (1, 'snapshot')");
        final Configuration.Builder configBuilder = keyTestConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL);
        // The server reports a NOT NULL UNIQUE candidate as a primary key. Other cases need an explicit key.
        if (!keyDefinition.contains("NOT NULL")) {
            configBuilder.with(BinlogConnectorConfig.MSG_KEY_COLUMNS, DATABASE.qualifiedTableName("keytest") + ":candidate");
        }
        final Configuration config = configBuilder.build();
        start(getConnectorClass(), config);
        final SourceRecord snapshot = consumeTable("keytest", 1).get(0);
        assertKeyRecord(snapshot, 1, "r");
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        executeStatements(DATABASE.getDatabaseName(), "UPDATE keytest SET payload = 'stream' WHERE candidate = 1");
        assertKeyRecord(consumeTable("keytest", 1).get(0), 1, "u");
        stopConnector();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        executeStatements(DATABASE.getDatabaseName(), "INSERT INTO keytest VALUES (2, 'restart')");
        assertKeyRecord(consumeTable("keytest", 1).get(0), 2, "c");
        assertThat(SCHEMA_HISTORY_PATH).doesNotExist();
    }

    @Test
    public void shouldIgnoreExcludedTableWithoutKey() throws Exception {
        executeStatements(DATABASE.getDatabaseName(), "CREATE TABLE keytest (candidate INT)");
        insertOrders(1);
        start(getConnectorClass(), metadataModeConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("orders")).build());
        assertThat(operationOf(consumeOrders(1).get(0))).isEqualTo("r");
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        executeStatements(DATABASE.getDatabaseName(), "INSERT INTO keytest VALUES (1)");
        insertOrders(1);
        assertThat(operationOf(consumeOrders(1).get(0))).isEqualTo("c");
    }

    @Test
    public void shouldRejectNewStreamingTableWithoutKey() throws Exception {
        final AtomicReference<Throwable> error = startWithErrorCapture(keyTestConfig().build());
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE keytest (candidate INT UNIQUE, payload VARCHAR(10))",
                "INSERT INTO keytest VALUES (1, 'stream')");

        assertKeyValidationFailure(error);
        assertNoRecordsToConsume();
    }

    @Test
    public void shouldRejectStreamingTableAfterPrimaryKeyDropped() throws Exception {
        executeStatements(DATABASE.getDatabaseName(), "CREATE TABLE keytest (candidate INT PRIMARY KEY, payload VARCHAR(10))");
        final AtomicReference<Throwable> error = startWithErrorCapture(keyTestConfig().build());
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        executeStatements(DATABASE.getDatabaseName(), "INSERT INTO keytest VALUES (1, 'before')");
        assertKeyRecord(consumeTable("keytest", 1).get(0), 1, "c");

        executeStatements(DATABASE.getDatabaseName(),
                "ALTER TABLE keytest DROP PRIMARY KEY",
                "INSERT INTO keytest VALUES (2, 'after')");
        assertKeyValidationFailure(error);
        assertNoRecordsToConsume();
    }

    @Test
    public void shouldRejectHistoricalTableMapWithoutKeyAfterPrimaryKeyAdded() throws Exception {
        executeStatements(DATABASE.getDatabaseName(), "CREATE TABLE keytest (candidate INT, payload VARCHAR(10))");
        final Configuration explicitKeyConfig = keyTestConfig()
                .with(BinlogConnectorConfig.MSG_KEY_COLUMNS, DATABASE.qualifiedTableName("keytest") + ":candidate").build();
        start(getConnectorClass(), explicitKeyConfig);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        executeStatements(DATABASE.getDatabaseName(), "INSERT INTO keytest VALUES (1, 'before')");
        assertKeyRecord(consumeTable("keytest", 1).get(0), 1, "c");
        stopConnector();
        final byte[] rewindPoint = java.nio.file.Files.readAllBytes(OFFSET_STORE_PATH);

        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO keytest VALUES (2, 'keyless')",
                "ALTER TABLE keytest ADD PRIMARY KEY (candidate)",
                "INSERT INTO keytest VALUES (3, 'keyed')");
        start(getConnectorClass(), explicitKeyConfig);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        assertThat(consumeTable("keytest", 2)).hasSize(2);
        stopConnector();

        // The current table has a PK, but the replayed TABLE_MAP does not. Startup-only validation is insufficient.
        java.nio.file.Files.write(OFFSET_STORE_PATH, rewindPoint);
        final AtomicReference<Throwable> error = startWithErrorCapture(keyTestConfig().build());
        assertKeyValidationFailure(error);
        assertNoRecordsToConsume();
    }

    @Test
    public void shouldAllowTableWithoutRecoverableKeyWithSchemaHistory() throws Exception {
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE keytest (candidate INT UNIQUE, payload VARCHAR(10))",
                "INSERT INTO keytest VALUES (1, 'snapshot')");
        start(getConnectorClass(), keyTestConfig()
                .with(BinlogConnectorConfig.BINLOG_METADATA_BASED_SCHEMA, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL).build());
        final SourceRecord snapshot = consumeTable("keytest", 1).get(0);
        assertThat(operationOf(snapshot)).isEqualTo("r");
        assertThat(snapshot.key()).isNull();
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        executeStatements(DATABASE.getDatabaseName(), "INSERT INTO keytest VALUES (2, 'stream')");
        final SourceRecord streamed = consumeTable("keytest", 1).get(0);
        assertThat(operationOf(streamed)).isEqualTo("c");
        assertThat(streamed.key()).isNull();
        assertThat(SCHEMA_HISTORY_PATH).exists();
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
    public void shouldResolveMixedCharsetsWhenEnumPrecedesStringColumns() throws Exception {
        final Configuration config = metadataModeConfig().build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        // These values round-trip only when each column is decoded with its own charset: the 'é' in
        // the latin1 column is the single byte 0xE9, which is not valid UTF-8. An ENUM column in
        // front of the string columns must not shift the per-column charset numbering.
        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO charset_mix (status, name_latin1, note_utf8) VALUES ('DONE', 'café', 'メモ')");

        final List<SourceRecord> records = consumeTable("charset_mix", 1);
        assertThat(records).hasSize(1);
        final Struct after = afterOf(records.get(0));
        assertThat(after.getString("status")).isEqualTo("DONE");
        assertThat(after.getString("name_latin1")).isEqualTo("café");
        assertThat(after.getString("note_utf8")).isEqualTo("メモ");

        stopConnector();
    }

    @Test
    public void shouldReconstructPrimaryKeyDefinedWithColumnPrefix() throws Exception {
        final Configuration config = metadataModeConfig().build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        executeStatements(DATABASE.getDatabaseName(), "INSERT INTO prefix_pk (code, qty) VALUES ('ABCDEFGH', 7)");

        final List<SourceRecord> records = consumeTable("prefix_pk", 1);
        assertThat(records).hasSize(1);
        final SourceRecord record = records.get(0);
        // A prefixed key is written as PRIMARY_KEY_WITH_PREFIX metadata and must still yield a keyed
        // record with the full column value.
        assertThat(record.key()).isNotNull();
        assertThat(((Struct) record.key()).getString("code")).isEqualTo("ABCDEFGH");
        assertThat(afterOf(record).getInt32("qty")).isEqualTo(7);

        stopConnector();
    }

    @Test
    public void shouldEmitSchemaChangeEventForTableNotYetRegisteredAfterRestart() throws Exception {
        final Configuration config = metadataModeConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        // Commit an offset past the initial schema snapshot, so that the restart below skips the
        // snapshot and streaming starts with an empty in-memory model.
        insertMarkerOrder("RS01");
        assertThat(consumeOrders(1)).hasSize(1);
        stopConnector();

        // While the connector is stopped, alter a table that will have produced no row event after
        // the restart: the DDL statement is then read before any TABLE_MAP event has registered the
        // table. It must neither kill the task nor be dropped from the schema change topic.
        executeStatements(DATABASE.getDatabaseName(), "ALTER TABLE all_types ADD COLUMN extra_after_restart INT");

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        // The schema change event carries the DDL statement; the table structure at that point is
        // unknown (it only arrives with the next TABLE_MAP event), so tableChanges stays empty.
        SourceRecord schemaChange = null;
        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (schemaChange == null && System.currentTimeMillis() < deadline) {
            final List<SourceRecord> changes = consumeRecordsByTopic(1).recordsForTopic(DATABASE.getServerName());
            if (changes != null && !changes.isEmpty()) {
                schemaChange = changes.get(0);
            }
        }
        assertThat(schemaChange).as("schema change event emitted for a not-yet-registered table").isNotNull();
        final Struct changeValue = (Struct) schemaChange.value();
        assertThat(changeValue.getString("ddl")).contains("extra_after_restart");
        assertThat(changeValue.getArray("tableChanges")).isEmpty();

        // The next TABLE_MAP event carries the post-DDL schema, so the row must decode with the new
        // column present.
        insertAllTypesRow();
        final List<SourceRecord> records = consumeTable("all_types", 1);
        assertThat(records).hasSize(1);
        final Struct after = afterOf(records.get(0));
        assertThat(after.schema().field("extra_after_restart")).as("column added while stopped").isNotNull();
        assertThat(after.getInt32("c_int")).isEqualTo(42);

        stopConnector();
    }

    @Test
    public void shouldCarryMaterializedDefaultValuesWithoutDeclaringSchemaDefaults() throws Exception {
        final Configuration config = metadataModeConfig().build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        // The status column is declared NOT NULL DEFAULT 'NEW' and omitted here, so the server
        // materializes the default into the written row.
        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO orders (quantity, price, code, note, created_at) VALUES (1, 1.000, 'DF01', 'default', NOW(3))");

        final List<SourceRecord> records = consumeOrders(1);
        assertThat(records).hasSize(1);
        final Struct after = afterOf(records.get(0));

        // Row values are complete: the change event carries the materialized default value.
        assertThat(after.getString("status")).isEqualTo("NEW");

        // The reconstructed schema declares no field default, because column DEFAULT values are not
        // part of the TABLE_MAP metadata. This pins the documented boundary of the mode: the column
        // stays a required field without a default, while the row data is unaffected.
        assertThat(after.schema().field("status").schema().defaultValue()).isNull();
        assertThat(after.schema().field("status").schema().isOptional()).isFalse();

        stopConnector();
    }

    @Test
    public void shouldStreamFromRewoundOffsetAcrossDdlChange() throws Exception {
        final Configuration config = metadataModeConfig().build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        // First change: consume it so that the committed offset lands right after this event. This
        // offset is the rewind point, taken while the table still has its original schema.
        insertMarkerOrder("RW01");
        assertThat(consumeOrders(1)).hasSize(1);
        stopConnector();
        // The inherited Testing.Files interface shadows java.nio.file.Files, hence the qualified name.
        final byte[] rewindPoint = java.nio.file.Files.readAllBytes(OFFSET_STORE_PATH);

        // While the connector keeps running past the rewind point, the table goes through
        // DML -> DDL -> DML, so the offset ends up beyond a schema change.
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        insertMarkerOrder("RW02");
        executeStatements(DATABASE.getDatabaseName(), "ALTER TABLE orders ADD COLUMN rewound INT");
        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO orders (quantity, status, price, code, note, created_at, rewound) "
                        + "VALUES (1, 'NEW', 1.000, 'RW03', 'post-ddl', NOW(3), 42)");
        assertThat(consumeOrders(2)).hasSize(2);
        stopConnector();

        // Rewind the offset behind the DDL statement. Because this mode keeps no persistent state other
        // than the offset, this is equivalent to an operator seeding a binlog position (or GTID set) for
        // a fresh connector, and it must work even though the current database schema no longer matches
        // the schema at the rewind point. A schema-history connector cannot do this without a history
        // topic that covers the target position.
        java.nio.file.Files.write(OFFSET_STORE_PATH, rewindPoint);

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        final List<SourceRecord> replayed = consumeOrders(2);
        assertThat(replayed).hasSize(2);

        // The replayed pre-DDL row must decode with the point-in-time schema carried by its TABLE_MAP
        // event, not with the current schema of the table.
        final Struct preDdl = afterOf(replayed.get(0));
        assertThat(preDdl.getString("code")).isEqualTo("RW02");
        assertThat(preDdl.schema().field("rewound")).as("pre-DDL row decoded with point-in-time schema").isNull();

        // The replayed post-DDL row must decode with the new schema.
        final Struct postDdl = afterOf(replayed.get(1));
        assertThat(postDdl.getString("code")).isEqualTo("RW03");
        assertThat(postDdl.getInt32("rewound")).isEqualTo(42);

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
    public void shouldStreamFromCurrentPositionWithNoDataSnapshot() throws Exception {
        // Rows that already exist before the connector starts. With a no_data snapshot the connector must
        // record the current binlog position (exactly as with a persistent schema history) and must NOT
        // replay these pre-existing rows.
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

    protected Configuration.Builder metadataModeConfig() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                // Schema change events are emitted exactly as with a persistent schema history; disabled
                // here so that the tests can consume data change records by plain counts.
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.BINLOG_METADATA_BASED_SCHEMA, true);
    }

    private Configuration.Builder keyTestConfig() {
        return metadataModeConfig().with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("keytest"));
    }

    private AtomicReference<Throwable> startWithErrorCapture(Configuration config) {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, throwable) -> {
            if (!success) {
                error.set(throwable);
            }
        });
        return error;
    }

    private void assertKeyValidationFailure(AtomicReference<Throwable> error) {
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertThat(error.get()).isNotNull());
        assertThat(error.get()).hasStackTraceContaining(DATABASE.qualifiedTableName("keytest"))
                .hasStackTraceContaining("binlog.metadata.based.schema")
                .hasStackTraceContaining("message.key.columns");
    }

    private static void assertKeyRecord(SourceRecord record, int candidate, String operation) {
        final Struct key = (Struct) record.key();
        assertThat(key).isNotNull();
        assertThat(key.schema().fields()).extracting(org.apache.kafka.connect.data.Field::name).containsExactly("candidate");
        assertThat(key.getInt32("candidate")).isEqualTo(candidate);
        assertThat(operationOf(record)).isEqualTo(operation);
    }

    private static final List<String> ALL_TYPES_COLUMNS = List.of(
            "id", "c_tinyint", "c_tinyint_u", "c_bool", "c_smallint", "c_smallint_u", "c_mediumint",
            "c_mediumint_u", "c_int", "c_int_u", "c_bigint", "c_bigint_u", "c_decimal", "c_float", "c_double",
            "c_bit", "c_date", "c_datetime", "c_timestamp", "c_time", "c_year", "c_char", "c_varchar",
            "c_binary", "c_varbinary", "c_tinytext", "c_text", "c_mediumtext", "c_longtext", "c_tinyblob",
            "c_blob", "c_mediumblob", "c_longblob", "c_enum", "c_set", "c_json");

    private List<SourceRecord> consumeOrders(int expected) throws InterruptedException {
        return consumeTable("orders", expected);
    }

    /**
     * Consume change records for the given table until {@code expected} of them have been collected or a
     * timeout elapses. This tolerates the streaming latency of a freshly started connector.
     */
    protected List<SourceRecord> consumeTable(String table, int expected) throws InterruptedException {
        final List<SourceRecord> collected = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (collected.size() < expected && System.currentTimeMillis() < deadline) {
            final SourceRecords records = consumeRecordsByTopic(expected - collected.size());
            final List<SourceRecord> forTable = records.recordsForTopic(DATABASE.topicForTable(table));
            if (forTable != null) {
                collected.addAll(forTable);
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

    private void insertAllTypesRow() throws SQLException {
        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO all_types (c_tinyint, c_tinyint_u, c_bool, c_smallint, c_smallint_u, c_mediumint, "
                        + "c_mediumint_u, c_int, c_int_u, c_bigint, c_bigint_u, c_decimal, c_float, c_double, c_bit, "
                        + "c_date, c_datetime, c_timestamp, c_time, c_year, c_char, c_varchar, c_binary, c_varbinary, "
                        + "c_tinytext, c_text, c_mediumtext, c_longtext, c_tinyblob, c_blob, c_mediumblob, c_longblob, "
                        + "c_enum, c_set, c_json) VALUES ("
                        + "-5, 200, 1, -30000, 60000, -8000000, 16000000, 42, 4000000000, 9000000000, "
                        + "18000000000000000000, 12345.678, 3.14, 2.718281828, b'10101', '2026-07-03', "
                        + "'2026-07-03 12:34:56.789', '2026-07-03 12:34:56.123456', '12:34:56.789', 2026, 'ABCD', "
                        + "'hello', 0x0102030405060708, 0xAABBCC, 'tiny', 'text', 'medium', 'long', 0x01, 0x02, 0x03, "
                        + "0x04, 'OK', 'a,c', JSON_OBJECT('k', 'v'))");
    }

    private void insertMarkerOrder(String code) throws SQLException {
        executeStatements(DATABASE.getDatabaseName(), String.format(
                "INSERT INTO orders (quantity, status, price, code, note, created_at) "
                        + "VALUES (1, 'NEW', 1.000, '%s', 'marker', NOW(3))",
                code));
    }

    protected static Struct afterOf(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    protected static String operationOf(SourceRecord record) {
        return ((Struct) record.value()).getString(Envelope.FieldName.OPERATION);
    }

    private static void assertSourceColumnType(Struct after, String columnName, String expectedType) {
        assertThat(after.schema().field(columnName).schema().parameters())
                .containsEntry(PropagateSourceMetadataToSchemaParameter.TYPE_NAME_PARAMETER_KEY, expectedType);
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
        // Selected types shared by the snapshot and streaming schemas.
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
