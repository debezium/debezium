/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotIsolationMode;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

public class SqlServerConnectorConfigTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorConfigTest.class);

    @Test
    void nullDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig().build());
        assertFalse(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    void emptyDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "")
                        .build());
        assertFalse(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    void nonEmptyDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "testDB1")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    void hostnameAndDefaultPortConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}:${port}");
    }

    @Test
    void hostnameAndPortConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .with(SqlServerConnectorConfig.PORT, "11433")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}:${port}");
    }

    @Test
    void hostnameAndInstanceConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .with(SqlServerConnectorConfig.INSTANCE, "instance")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}\\instance");
    }

    @Test
    void hostnameAndInstanceAndPortConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .with(SqlServerConnectorConfig.INSTANCE, "instance")
                        .with(SqlServerConnectorConfig.PORT, "11433")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}\\instance:${port}");
    }

    @Test
    void validQueryFetchSizeDefaults() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .build());
        assertEquals(connectorConfig.getQueryFetchSize(), 10_000);
    }

    @Test
    void validQueryFetchSizeAvailable() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .with(SqlServerConnectorConfig.QUERY_FETCH_SIZE, 20_000)
                        .build());
        assertEquals(connectorConfig.getQueryFetchSize(), 20_000);
    }

    @Test
    @FixFor("debezium/dbz#1387")
    void readOnlyApplicationIntentDefaultsToSnapshotIsolation() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(CommonConnectorConfig.DRIVER_CONFIG_PREFIX + "applicationIntent", "ReadOnly")
                        .build());
        assertTrue(connectorConfig.isReadOnlyDatabaseConnection());
        assertEquals(SnapshotIsolationMode.SNAPSHOT, connectorConfig.getSnapshotIsolationMode());
    }

    @Test
    @FixFor("debezium/dbz#1387")
    void readOnlyApplicationIntentHonorsConfiguredSnapshotIsolationMode() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(CommonConnectorConfig.DRIVER_CONFIG_PREFIX + "applicationIntent", "ReadOnly")
                        .with(SqlServerConnectorConfig.SNAPSHOT_ISOLATION_MODE, SnapshotIsolationMode.READ_COMMITTED)
                        .build());
        assertTrue(connectorConfig.isReadOnlyDatabaseConnection());
        assertEquals(SnapshotIsolationMode.READ_COMMITTED, connectorConfig.getSnapshotIsolationMode());
    }

    @Test
    @FixFor("debezium/dbz#1387")
    void readOnlyApplicationIntentHonorsExplicitlyConfiguredDefaultSnapshotIsolationMode() {
        // repeatable_read is the field default, so this proves the check is "was the key set" and not "differs from default"
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(CommonConnectorConfig.DATABASE_CONFIG_PREFIX + "applicationIntent", "ReadOnly")
                        .with(SqlServerConnectorConfig.SNAPSHOT_ISOLATION_MODE, SnapshotIsolationMode.REPEATABLE_READ)
                        .build());
        assertTrue(connectorConfig.isReadOnlyDatabaseConnection());
        assertEquals(SnapshotIsolationMode.REPEATABLE_READ, connectorConfig.getSnapshotIsolationMode());
    }

    @Test
    @FixFor("debezium/dbz#1387")
    void snapshotIsolationModeDefaultsToRepeatableReadWithoutReadOnlyApplicationIntent() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig().build());
        assertFalse(connectorConfig.isReadOnlyDatabaseConnection());
        assertEquals(SnapshotIsolationMode.REPEATABLE_READ, connectorConfig.getSnapshotIsolationMode());
    }

    @Test
    void captureInstanceFilterAppliesIncludeAndExcludeLists() {
        // No list configured: every capture instance is accepted.
        assertTrue(captureInstanceFilter(null, null).test("anything"));

        // Include list: only matching instances are accepted.
        final Predicate<String> included = captureInstanceFilter("dbo_tablea,dbo_tableb", null);
        assertTrue(included.test("dbo_tablea"));
        assertTrue(included.test("dbo_tableb"));
        assertFalse(included.test("dbo_tablec"));

        // Exclude list: matching instances are rejected, everything else accepted.
        final Predicate<String> excluded = captureInstanceFilter(null, "dbo_tableb");
        assertFalse(excluded.test("dbo_tableb"));
        assertTrue(excluded.test("dbo_tablea"));
    }

    @Test
    void captureInstanceFilterMatchingIsAnchoredAndCaseInsensitive() {
        final Predicate<String> filter = captureInstanceFilter(null, "dbo_table.*");
        // Case-insensitive.
        assertFalse(filter.test("DBO_TableA"));
        // Anchored: a leading segment that is not part of the pattern must not match.
        assertTrue(filter.test("other_dbo_tablea"));
    }

    @Test
    void captureInstanceIncludeAndExcludeListsAreMutuallyExclusive() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "testDB1")
                        .with(SqlServerConnectorConfig.CAPTURE_INSTANCE_INCLUDE_LIST, "dbo_tablea")
                        .with(SqlServerConnectorConfig.CAPTURE_INSTANCE_EXCLUDE_LIST, "dbo_tableb")
                        .build());
        assertFalse(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    @FixFor("debezium/dbz#2867")
    void historyComparatorUsesCommitLsnWhenChangeLsnIsUnavailable() {
        final HistoryRecordComparator comparator = new SqlServerConnectorConfig(defaultConfig().build())
                .getHistoryRecordComparator();
        final Map<String, ?> source = Map.of("server", "server");

        final HistoryRecord snapshot = historyRecord(source, "NULL", "NULL");
        final HistoryRecord streamedChange = historyRecord(source, "0000001a:00000010:0005", "0000001a:00000010:0001");
        final HistoryRecord laterChange = historyRecord(source, "0000002a:00000001:0001", "0000002a:00000001:0001");
        final HistoryRecord heartbeatOffset = historyRecord(source, "00000020:00000001:0001", "NULL");
        final HistoryRecord snapshotOffset = historyRecord(source, "NULL", "NULL");

        final HistoryRecord changeAfterCommitBeforeHeartbeat = historyRecord(source, "00000018:00000001:0001", "00000028:00000001:0001");

        assertTrue(comparator.isAtOrBefore(snapshot, heartbeatOffset));
        assertTrue(comparator.isAtOrBefore(streamedChange, heartbeatOffset));
        assertTrue(comparator.isAtOrBefore(changeAfterCommitBeforeHeartbeat, heartbeatOffset));
        assertFalse(comparator.isAtOrBefore(laterChange, heartbeatOffset));
        assertTrue(comparator.isAtOrBefore(snapshot, snapshotOffset));

        final HistoryRecord changeOffset = historyRecord(source, "0000002a:00000001:0005", "0000001a:00000010:0001");
        assertTrue(comparator.isAtOrBefore(streamedChange, changeOffset));
        assertTrue(comparator.isAtOrBefore(streamedChange, streamedChange));
        assertFalse(comparator.isAtOrBefore(laterChange, changeOffset));

        final HistoryRecord recordedNullChange = historyRecord(source, "0000001a:00000010:0005", "NULL");
        assertTrue(comparator.isAtOrBefore(recordedNullChange, changeOffset));
        final HistoryRecord laterCommitWithUnavailableChange = historyRecord(source, "0000002a:00000001:0001", "NULL");
        final HistoryRecord earlierUnavailableOffset = historyRecord(source, "00000020:00000001:0001", "NULL");
        assertFalse(comparator.isAtOrBefore(laterCommitWithUnavailableChange, earlierUnavailableOffset));
        assertFalse(comparator.isAtOrBefore(streamedChange, historyRecord(Map.of("server", "other"), "0000002a:00000001:0001", "NULL")));
    }

    @Test
    @FixFor("debezium/dbz#2867")
    void historyRecoveryAppliesChangesAtOrBeforeAHeartbeatOffset() throws InterruptedException {
        final MemorySchemaHistory history = new MemorySchemaHistory();
        history.configure(Configuration.create().build(),
                new SqlServerConnectorConfig(defaultConfig().build()).getHistoryRecordComparator(),
                SchemaHistoryListener.NOOP, false);
        history.start();

        final Map<String, Object> source = new HashMap<>();
        source.put("server", "server");
        final TableId tableId = new TableId("db", "dbo", "orders");
        history.record(source, position("NULL", "NULL"), "db", "dbo", null, tableChange(tableId, "id"), Instant.EPOCH);
        history.record(source, position("0000001a:00000010:0005", "0000001a:00000010:0001"), "db", "dbo", null,
                tableChange(tableId, "id", "status"), Instant.EPOCH);
        history.record(source, position("00000018:00000001:0001", "00000028:00000001:0001"), "db", "dbo", null,
                tableChange(tableId, "id", "status", "amount"), Instant.EPOCH);
        history.record(source, position("0000002a:00000001:0001", "0000002a:00000001:0001"), "db", "dbo", null,
                tableChange(tableId, "id", "status", "amount", "note"), Instant.EPOCH);

        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(defaultConfig().build());
        final SqlServerOffsetContext offsetContext = new SqlServerOffsetContext(connectorConfig,
                TxLogPosition.valueOf(Lsn.valueOf("00000010:00000001:0001"), null), null, false);
        offsetContext.setChangePosition(TxLogPosition.valueOf(Lsn.valueOf("00000020:00000001:0001"), (Integer) null), 0);
        final Map<String, ?> heartbeatOffset = offsetContext.getOffset();
        assertEquals("NULL", heartbeatOffset.get(SourceInfo.CHANGE_LSN_KEY));

        final Tables recovered = new Tables();
        history.recover(Map.of(source, heartbeatOffset), recovered, null);
        assertEquals(List.of("id", "status", "amount"), recovered.forTable(tableId).retrieveColumnNames());

        final Tables otherSource = new Tables();
        history.recover(Map.of(Map.of("server", "other"), heartbeatOffset), otherSource, null);
        assertNull(otherSource.forTable(tableId));
    }

    @Test
    @FixFor("debezium/dbz#2867")
    void schemaMigrationRecordsTheNewCaptureStartLsn() throws InterruptedException {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(defaultConfig().build());
        final Lsn previous = Lsn.valueOf("00000010:00000001:0001");
        final Lsn migration = Lsn.valueOf("00000030:00000001:0001");
        final SqlServerOffsetContext offsetContext = new SqlServerOffsetContext(connectorConfig,
                TxLogPosition.valueOf(previous, previous, 7), null, true);
        offsetContext.setChangePosition(TxLogPosition.valueOf(previous, previous, 7), 4);

        final SqlServerChangeTable newTable = new SqlServerChangeTable(new TableId("db", "dbo", "orders"), "dbo_orders", 1, migration,
                Collections.emptyList());
        final Map<String, ?>[] recorded = new Map[1];
        final long eventSerialNo = offsetContext.getEventSerialNo();
        SqlServerStreamingChangeEventSource.withCaptureStartPosition(offsetContext, newTable,
                () -> recorded[0] = offsetContext.getOffset());

        assertEquals(migration.toString(), recorded[0].get(SourceInfo.COMMIT_LSN_KEY));
        assertEquals(migration.toString(), recorded[0].get(SourceInfo.CHANGE_LSN_KEY));
        assertEquals(7, recorded[0].get(SourceInfo.COMMAND_ID_KEY));
        assertEquals(previous, offsetContext.getChangePosition().getCommitLsn());
        assertEquals(previous, offsetContext.getChangePosition().getInTxLsn());
        assertEquals(eventSerialNo, offsetContext.getEventSerialNo());
        assertEquals(7, offsetContext.getChangePosition().getCommandId());
    }

    @Test
    @FixFor("debezium/dbz#2867")
    void schemaMigrationRestoresSerialWhenCaptureStartMatchesCurrentPosition() throws InterruptedException {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(defaultConfig().build());
        final Lsn migration = Lsn.valueOf("00000030:00000001:0001");
        final SqlServerOffsetContext offsetContext = new SqlServerOffsetContext(connectorConfig,
                TxLogPosition.valueOf(migration, migration, 7), null, true);
        offsetContext.setChangePosition(TxLogPosition.valueOf(migration, migration, 7), 4);
        final long eventSerialNo = offsetContext.getEventSerialNo();

        final SqlServerChangeTable newTable = new SqlServerChangeTable(new TableId("db", "dbo", "orders"), "dbo_orders", 1, migration,
                Collections.emptyList());
        SqlServerStreamingChangeEventSource.withCaptureStartPosition(offsetContext, newTable,
                () -> assertEquals(0L, offsetContext.getEventSerialNo()));

        assertEquals(migration, offsetContext.getChangePosition().getCommitLsn());
        assertEquals(migration, offsetContext.getChangePosition().getInTxLsn());
        assertEquals(7, offsetContext.getChangePosition().getCommandId());
        assertEquals(eventSerialNo, offsetContext.getEventSerialNo());
    }

    private static Map<String, Object> position(String commitLsn, String changeLsn) {
        final Map<String, Object> position = new HashMap<>();
        position.put(SourceInfo.COMMIT_LSN_KEY, commitLsn);
        position.put(SourceInfo.CHANGE_LSN_KEY, changeLsn);
        return position;
    }

    private static TableChanges tableChange(TableId tableId, String... columnNames) {
        final TableEditor editor = Table.editor().tableId(tableId);
        for (final String columnName : columnNames) {
            editor.addColumn(Column.editor().name(columnName).type("int").create());
        }
        final TableChanges changes = new TableChanges();
        changes.alter(editor.create());
        return changes;
    }

    private static HistoryRecord historyRecord(Map<String, ?> source, String commitLsn, String changeLsn) {
        return new HistoryRecord(source, position(commitLsn, changeLsn), null, null, null, null, null);
    }

    private Predicate<String> captureInstanceFilter(String includeList, String excludeList) {
        final Configuration.Builder config = defaultConfig();
        if (includeList != null) {
            config.with(SqlServerConnectorConfig.CAPTURE_INSTANCE_INCLUDE_LIST, includeList);
        }
        if (excludeList != null) {
            config.with(SqlServerConnectorConfig.CAPTURE_INSTANCE_EXCLUDE_LIST, excludeList);
        }
        return new SqlServerConnectorConfig(config.build()).getCaptureInstanceFilter();
    }

    private Configuration.Builder defaultConfig() {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                .with(SqlServerConnectorConfig.HOSTNAME, "localhost")
                .with(SqlServerConnectorConfig.USER, "debezium")
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                .with(KafkaSchemaHistory.TOPIC, "history");
    }

    private String connectionUrl(SqlServerConnectorConfig connectorConfig) {
        SqlServerJdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
        return SqlServerConnection.createUrlPattern(jdbcConfig, false);
    }
}
