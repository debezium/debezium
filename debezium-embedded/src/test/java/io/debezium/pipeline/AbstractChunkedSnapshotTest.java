/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalSnapshotChangeEventSource;

/**
 * An abstract base class for the new chunked-based table snapshot feature.
 *
 * @author Chris Cranford
 */
public abstract class AbstractChunkedSnapshotTest<T extends SourceConnector> extends AbstractAsyncEngineConnectorTest {

    protected LogInterceptor logInterceptor;

    @BeforeEach
    public void beforeEach() throws Exception {
        logInterceptor = new LogInterceptor(RelationalSnapshotChangeEventSource.class);
    }

    @AfterEach
    public void afterEach() throws Exception {
        logInterceptor = null;
    }

    @Test
    @FixFor("dbz#1220")
    public void shouldSnapshotUsingOneThreadPerTableLegacyBehavior() throws Exception {
        final int ROW_COUNT = 10_000;

        final List<String> tableNames = getMultipleSingleKeyTableNames();
        for (String tableName : tableNames) {
            createSingleKeyTable(tableName);
            populateSingleKeyTable(tableName, ROW_COUNT);
        }

        final Configuration config = getConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                .with(CommonConnectorConfig.LEGACY_SNAPSHOT_MAX_THREADS, Boolean.TRUE)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, getMultipleSingleKeyCollectionNames())
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, ROW_COUNT)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, ROW_COUNT * tableNames.size() + 1)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        final SourceRecords allRecords = consumeRecordsByTopic(ROW_COUNT * tableNames.size());
        for (String tableName : tableNames) {
            final List<SourceRecord> records = allRecords.recordsForTopic(getTableTopicName(tableName));
            assertThat(records).hasSize(ROW_COUNT);

            final Collection<?> keys = getRecordKeysForSingleKeyTable(records, getSingleKeyTableKeyColumnName());
            assertThat(keys).hasSize(ROW_COUNT);
        }

        assertThat(logInterceptor.containsMessage("Creating snapshot worker pool with 2 worker thread(s)")).isTrue();
    }

    @Test
    @FixFor("dbz#1220")
    public void shouldSnapshotKeylessTableUsingLegacyTablePerThreadStrategy() throws Exception {
        final int ROW_COUNT = 15_000;

        final List<String> tableNames = getMultipleSingleKeyTableNames();
        for (int i = 0; i < tableNames.size(); i++) {
            final String tableName = tableNames.get(i);
            if (i == 0) {
                createKeylessTable(tableName);
                populateSingleKeylessTable(tableName, ROW_COUNT);
            }
            else {
                createSingleKeyTable(tableName);
                populateSingleKeyTable(tableName, ROW_COUNT);
            }
        }

        final Configuration config = getConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, getMultipleSingleKeyCollectionNames())
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, ROW_COUNT)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, ROW_COUNT * tableNames.size() + 1)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        final SourceRecords allRecords = consumeRecordsByTopic(ROW_COUNT * tableNames.size());
        for (String tableName : tableNames) {
            final List<SourceRecord> records = allRecords.recordsForTopic(getTableTopicName(tableName));
            assertThat(records).hasSize(ROW_COUNT);

            final Collection<?> keys = getRecordKeysForSingleKeyTable(records, getSingleKeyTableKeyColumnName());
            assertThat(keys).hasSize(ROW_COUNT);
        }

        assertCreatedChunkSnapshotWorker(2);
        assertKeylessTableSnapshotChunked(getFullyQualifiedTableName(tableNames.get(0)));
        for (int i = 1; i < tableNames.size(); i++) {
            assertTableSnapshotChunked(getFullyQualifiedTableName(tableNames.get(i)), 1, 2);
        }

        assertThat(logInterceptor.containsMessage(
                "Finished chunk snapshot of %d tables (%d chunks)".formatted(
                        tableNames.size(), ((tableNames.size() - 1) * 2) + 1)))
                .isTrue();
    }

    @Test
    @FixFor("dbz#1220")
    public void shouldSnapshotUsingPerTableMultiplierOverrides() throws Exception {
        final int ROW_COUNT = 10_000;

        final String tableName = getSingleKeyTableName();
        final String qualifiedTableName = getFullyQualifiedTableName(tableName);

        createSingleKeyTable(tableName);
        populateSingleKeyTable(tableName, ROW_COUNT);

        final Configuration config = getConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, getSingleKeyCollectionName())
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, ROW_COUNT)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, ROW_COUNT + 1)
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS_MULTIPLIER.name() + "." + qualifiedTableName, 5)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        final SourceRecords allRecords = consumeRecordsByTopic(ROW_COUNT);

        final List<SourceRecord> records = allRecords.recordsForTopic(getTableTopicName(tableName));
        assertThat(records).hasSize(ROW_COUNT);

        final Collection<?> keys = getRecordKeysForSingleKeyTable(records, getSingleKeyTableKeyColumnName());
        assertThat(keys).hasSize(ROW_COUNT);

        assertCreatedChunkSnapshotWorker(2);
        assertTableSnapshotChunked(qualifiedTableName, 5, 10);
        assertChunkedSnapshotFinished(1, 10);
    }

    @Test
    @FixFor("dbz#1220")
    public void shouldSnapshotCompositeKeyTable() throws Exception {
        final int ROW_COUNT = 10_000;

        final String tableName = getCompositeKeyTableName();
        final String qualifiedTableName = getFullyQualifiedTableName(tableName);

        createCompositeKeyTable(tableName);
        populateCompositeKeyTable(tableName, ROW_COUNT);

        final Configuration config = getConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS_MULTIPLIER, 5)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, getCompositeKeyCollectionName())
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, ROW_COUNT)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, ROW_COUNT + 1)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        final SourceRecords allRecords = consumeRecordsByTopic(ROW_COUNT);

        final List<SourceRecord> records = allRecords.recordsForTopic(getTableTopicName(tableName));
        assertThat(records).hasSize(ROW_COUNT);

        final Collection<?> keys = getRecordKeysForCompositeKeyTable(records, getCompositeKeyTableKeyColumnNames());
        assertThat(keys).hasSize(ROW_COUNT);

        assertCreatedChunkSnapshotWorker(2);
        assertTableSnapshotChunked(qualifiedTableName, 5, 10);
        assertChunkedSnapshotFinished(1, 10);
    }

    @Test
    @FixFor("dbz#1220")
    public void shouldSnapshotMultipleTablesChunkedWithVaryingRowCounts() throws Exception {
        int totalRows = 0;

        final Map<String, Integer> tableRowCounts = new HashMap<>();
        final List<String> tableNames = getMultipleSingleKeyTableNames();
        for (String tableName : tableNames) {
            final int tableRowCount = (int) (Math.random() * (10_000 - 2_500 + 1));
            tableRowCounts.put(tableName, tableRowCount);
            totalRows += tableRowCount;

            createSingleKeyTable(tableName);
            populateSingleKeyTable(tableName, tableRowCount);
        }

        final Configuration config = getConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 5)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, getMultipleSingleKeyCollectionNames())
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, totalRows)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, totalRows + 1)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        final SourceRecords allRecords = consumeRecordsByTopic(totalRows);

        int tableIndex = 0;
        for (String tableName : tableNames) {
            final int tableRowCount = tableRowCounts.get(tableName);

            final List<SourceRecord> records = allRecords.recordsForTopic(getTableTopicName(tableName));
            assertThat(records).hasSize(tableRowCount);

            final Collection<?> keys = getRecordKeysForSingleKeyTable(records, getSingleKeyTableKeyColumnName());
            assertThat(keys).hasSize(tableRowCount);

            if (tableIndex == 0) {
                assertRecordsSnapshotMarkers(records, SnapshotRecord.FIRST, SnapshotRecord.LAST_IN_DATA_COLLECTION);
            }
            else if (tableIndex == tableNames.size() - 1) {
                assertRecordsSnapshotMarkers(records, SnapshotRecord.FIRST_IN_DATA_COLLECTION, SnapshotRecord.LAST);
            }
            else {
                assertRecordsSnapshotMarkers(records, SnapshotRecord.FIRST_IN_DATA_COLLECTION, SnapshotRecord.LAST_IN_DATA_COLLECTION);
            }

            tableIndex++;
        }

        assertCreatedChunkSnapshotWorker(5);
        assertChunkedSnapshotFinished(tableRowCounts.size(), tableRowCounts.size() * 5);
    }

    @Test
    @FixFor("dbz#1220")
    @Disabled
    public void shouldSnapshotChunkedPerformanceTest() throws Exception {
        final int ROW_COUNT = 10_000_000;

        final String tableName = getSingleKeyTableName();
        createSingleKeyTable(tableName);
        populateSingleKeyTable(tableName, ROW_COUNT);

        final Configuration config = getConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 20)
                // .with(CommonConnectorConfig.LEGACY_SNAPSHOT_MAX_THREADS, Boolean.TRUE)
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS_MULTIPLIER, 5)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, getSingleKeyCollectionName())
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, ROW_COUNT / 16)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, ROW_COUNT + 1)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        final SourceRecords allRecords = consumeRecordsByTopic(ROW_COUNT);

        final List<SourceRecord> records = allRecords.recordsForTopic(getTableTopicName(tableName));
        assertThat(records).hasSize(ROW_COUNT);

        final Collection<?> keys = getRecordKeysForSingleKeyTable(records, getSingleKeyTableKeyColumnName());
        assertThat(keys).hasSize(ROW_COUNT);

        assertRecordsSnapshotMarkers(records, SnapshotRecord.FIRST, SnapshotRecord.LAST);
    }

    @SuppressWarnings("SqlSourceToSinkFlow")
    protected void populateSingleKeyTable(String tableName, int rowCount) throws SQLException {
        final JdbcConnection connection = getConnection();
        try (PreparedStatement st = connection.connection().prepareStatement("INSERT INTO " + tableName + " VALUES (?,?)")) {
            for (int i = 0; i < rowCount; i++) {
                st.setInt(1, i);
                st.setString(2, String.valueOf(i));
                st.addBatch();
            }
            st.executeBatch();
        }
        connection.commit();
    }

    @SuppressWarnings("SameParameterValue")
    protected void populateSingleKeylessTable(String tableName, int rowCount) throws SQLException {
        // Logically there is no difference, reuse
        populateSingleKeyTable(tableName, rowCount);
    }

    @SuppressWarnings({ "SqlSourceToSinkFlow", "SameParameterValue" })
    protected void populateCompositeKeyTable(String tableName, int rowCount) throws SQLException {
        final JdbcConnection connection = getConnection();
        try (PreparedStatement st = connection.connection().prepareStatement("INSERT INTO " + tableName + " VALUES (?,?,?)")) {
            for (int i = 0; i < rowCount; i++) {
                st.setInt(1, i);
                st.setString(2, String.valueOf(i));
                st.setString(3, String.valueOf(i));
                st.addBatch();
            }
            st.executeBatch();
        }
        connection.commit();
    }

    protected String getSingleKeyTableName() {
        return "dbz1220";
    }

    protected String getCompositeKeyTableName() {
        return "dbz1220";
    }

    protected List<String> getMultipleSingleKeyTableNames() {
        return List.of("dbz1220a", "dbz1220b", "dbz1220c", "dbz1220d");
    }

    protected Collection<?> getRecordKeysForSingleKeyTable(List<SourceRecord> records, String keyColumnName) {
        return records.stream().map(r -> {
            final Struct after = ((Struct) r.value()).getStruct(Envelope.FieldName.AFTER);
            return after.get(keyColumnName);
        }).collect(Collectors.toSet());
    }

    protected Collection<?> getRecordKeysForCompositeKeyTable(List<SourceRecord> records, List<String> keyColumnNames) {
        return records.stream().map(r -> {
            final Struct after = ((Struct) r.value()).getStruct(Envelope.FieldName.AFTER);
            final Map<String, Object> keyValues = new LinkedHashMap<>();
            for (String keyColumnName : keyColumnNames) {
                keyValues.put(keyColumnName, after.get(keyColumnName));
            }
            return keyValues;
        }).collect(Collectors.toSet());
    }

    protected void assertCreatedChunkSnapshotWorker(int threadCount) {
        assertThat(logInterceptor.containsMessage(
                "Creating chunked snapshot worker pool with %d worker thread(s)".formatted(threadCount))).isTrue();
    }

    protected void assertTableSnapshotChunked(String tableName, int multiplier, int chunks) {
        assertThat(logInterceptor.containsMessage(
                "Table '%s' calculating chunk boundaries using multiplier %d with %d chunks".formatted(tableName, multiplier, chunks))).isTrue();
    }

    protected void assertKeylessTableSnapshotChunked(String tableName) {
        assertThat(logInterceptor.containsMessage(
                "Table '%s' has no key columns, using single chunk.".formatted(tableName))).isTrue();
    }

    protected void assertChunkedSnapshotFinished(int tableCount, int chunkCount) {
        assertThat(logInterceptor.containsMessage(
                "Finished chunk snapshot of %d tables (%d chunks)".formatted(tableCount, chunkCount))).isTrue();
    }

    protected void assertRecordsSnapshotMarkers(List<SourceRecord> records, SnapshotRecord first, SnapshotRecord last) {
        assertThat(records).hasSizeGreaterThan(1);

        final Struct firstSource = getSourceFromRecord(records.get(0));
        assertThat(firstSource.get(AbstractSourceInfo.SNAPSHOT_KEY)).isEqualTo(first.toString().toLowerCase());

        final Struct lastSource = getSourceFromRecord(records.get(records.size() - 1));
        assertThat(lastSource.get(AbstractSourceInfo.SNAPSHOT_KEY)).isEqualTo(last.toString().toLowerCase());
    }

    protected Struct getSourceFromRecord(SourceRecord record) {
        final Struct value = (Struct) record.value();
        return value.getStruct(Envelope.FieldName.SOURCE);
    }

    protected abstract Class<T> getConnectorClass();

    protected abstract JdbcConnection getConnection();

    protected abstract Configuration.Builder getConfig();

    protected abstract void waitForSnapshotToBeCompleted() throws InterruptedException;

    protected abstract String getSingleKeyCollectionName();

    protected abstract String getCompositeKeyCollectionName();

    protected abstract String getMultipleSingleKeyCollectionNames();

    protected abstract void createSingleKeyTable(String tableName) throws SQLException;

    protected abstract void createCompositeKeyTable(String tableName) throws SQLException;

    protected abstract void createKeylessTable(String tableName) throws SQLException;

    protected abstract String getSingleKeyTableKeyColumnName();

    protected abstract List<String> getCompositeKeyTableKeyColumnNames();

    protected abstract String getTableTopicName(String tableName);

    protected abstract String getFullyQualifiedTableName(String tableName);
}
