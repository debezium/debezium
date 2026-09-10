/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.chunked.SnapshotChunk;
import io.debezium.pipeline.source.snapshot.chunked.SnapshotProgress;
import io.debezium.pipeline.source.snapshot.chunked.TableChunkProgress;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Verifies the per-table/per-chunk snapshot retry of {@link RelationalSnapshotChangeEventSource}: a
 * transient (connector-classified retriable) read failure is retried against the whole-snapshot
 * {@code snapshot.retry.max} budget and resumes from the last emitted key, so no rows are emitted twice
 * and the FIRST/LAST snapshot markers stay unique; keyless tables fall back to a re-read that accepts
 * duplicates; {@code 0} keeps the pre-existing fail-fast behavior; non-retriable failures do not consume
 * the budget; an interruption always aborts.
 */
public class RelationalSnapshotChangeEventSourceRetryTest {

    private static final String BASE_SELECT = "SELECT * FROM \"s1\".\"table1\"";
    private static final String RETRIABLE_SQL_STATE = "08S01";

    private static final Object[] ROW_1 = { 1, "a" };
    private static final Object[] ROW_2 = { 2, "b" };
    private static final Object[] ROW_3 = { 3, "c" };

    private Connection sqlConnection;
    private FakeJdbcConnection jdbcConnection;
    private ChangeEventSourceContext sourceContext;
    private Partition partition;
    private OffsetContext offset;
    private SnapshotReceiver<Partition> snapshotReceiver;
    private NotificationService<Partition, OffsetContext> notificationService;

    private final AtomicReference<SnapshotRecord> currentMarker = new AtomicReference<>();
    private final List<Object[]> emittedRows = new ArrayList<>();
    private final List<SnapshotRecord> emittedMarkers = new ArrayList<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        sqlConnection = mock(Connection.class);
        jdbcConnection = new FakeJdbcConnection(config(0).getJdbcConfig(), sqlConnection);
        sourceContext = mock(ChangeEventSourceContext.class);
        when(sourceContext.isRunning()).thenReturn(true);
        partition = mock(Partition.class);
        offset = mock(OffsetContext.class);
        doAnswer(invocation -> {
            currentMarker.set(invocation.getArgument(0));
            return null;
        }).when(offset).markSnapshotRecord(any(SnapshotRecord.class));
        snapshotReceiver = mock(SnapshotReceiver.class);
        notificationService = mock(NotificationService.class, RETURNS_DEEP_STUBS);
    }

    @Test
    @FixFor("debezium/dbz#2297")
    public void chunkRetriesTransientFailuresAndResumesFromLastEmittedKey() throws Exception {
        final TestSnapshotSource source = newSource(3);
        final Table table = keyedTable();

        // Attempt 1 emits ROW_1 and then fails reading the next row; attempt 2 fails before reading any
        // row; attempt 3 returns the remaining rows after the resume key.
        final ResultSet resultSetForStatement1 = mockResultSet(new Object[][]{ ROW_1, ROW_2 }, 3, retriableError());
        final PreparedStatement statement1 = mock(PreparedStatement.class);
        when(statement1.executeQuery()).thenReturn(resultSetForStatement1);
        final PreparedStatement statement2 = mock(PreparedStatement.class);
        when(statement2.executeQuery()).thenThrow(retriableError());
        final ResultSet resultSetForStatement3 = mockResultSet(new Object[][]{ ROW_2, ROW_3 }, null, null);
        final PreparedStatement statement3 = mock(PreparedStatement.class);
        when(statement3.executeQuery()).thenReturn(resultSetForStatement3);
        when(sqlConnection.prepareStatement(anyString())).thenReturn(statement1, statement2, statement3);

        chunkedCallable(source, keyedSingleChunk(table)).call();

        // Every row is emitted exactly once and the FIRST/LAST markers are unique
        assertThat(emittedRows).containsExactly(ROW_1, ROW_2, ROW_3);
        assertThat(emittedMarkers).containsExactly(SnapshotRecord.FIRST, SnapshotRecord.TRUE, SnapshotRecord.LAST);

        // Both retries resume strictly after the last emitted key instead of re-reading the chunk
        final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(sqlConnection, times(3)).prepareStatement(sqlCaptor.capture());
        assertThat(sqlCaptor.getAllValues().get(0))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE NOT (\"pk1\" > ?) ORDER BY \"pk1\"");
        assertThat(sqlCaptor.getAllValues().get(1))
                .isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE (\"pk1\" > ?) AND NOT (\"pk1\" > ?) ORDER BY \"pk1\"");
        assertThat(sqlCaptor.getAllValues().get(2)).isEqualTo(sqlCaptor.getAllValues().get(1));
        verify(statement3).setObject(1, 1);
        verify(statement3).setObject(2, 10);

        // The connection is recovered before each retry and each retry is surfaced as a notification
        assertThat(jdbcConnection.rollbackCount).isEqualTo(2);
        verify(notificationService.initialSnapshotNotificationService(), times(2))
                .notifyTableSnapshotRetry(any(), any(), eq("s1.table1"), anyInt(), eq(3));
    }

    @Test
    @FixFor("debezium/dbz#2297")
    public void chunkFailsWhenRetryBudgetIsExhausted() throws Exception {
        final TestSnapshotSource source = newSource(2);
        final Table table = keyedTable();

        final PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenThrow(retriableError());
        when(sqlConnection.prepareStatement(anyString())).thenReturn(statement);

        assertThatThrownBy(chunkedCallable(source, keyedSingleChunk(table))::call)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("s1.table1_chunk_0")
                .hasCauseInstanceOf(SQLException.class);

        // One initial attempt plus the two budgeted retries
        verify(sqlConnection, times(3)).prepareStatement(anyString());
        assertThat(emittedRows).isEmpty();
        verify(notificationService.initialSnapshotNotificationService())
                .notifyCompletedTableWithError(any(), any(), eq("s1.table1"));
    }

    @Test
    @FixFor("debezium/dbz#2297")
    public void zeroRetriesReproducesCurrentBehavior() throws Exception {
        final TestSnapshotSource source = newSource(0);
        final Table table = keyedTable();

        final PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenThrow(retriableError());
        when(sqlConnection.prepareStatement(anyString())).thenReturn(statement);

        assertThatThrownBy(chunkedCallable(source, keyedSingleChunk(table))::call)
                .isInstanceOf(ConnectException.class)
                .hasCauseInstanceOf(SQLException.class);

        // A single attempt, no connection recovery, no retry notification
        verify(sqlConnection, times(1)).prepareStatement(anyString());
        assertThat(jdbcConnection.rollbackCount).isZero();
        verify(notificationService.initialSnapshotNotificationService(), never())
                .notifyTableSnapshotRetry(any(), any(), anyString(), anyInt(), anyInt());
    }

    @Test
    @FixFor("debezium/dbz#2297")
    public void nonRetriableErrorFailsImmediatelyWithoutConsumingTheBudget() throws Exception {
        final TestSnapshotSource source = newSource(1);
        final Table table = keyedTable();

        // The first chunk fails with a non-retriable error and consumes no budget
        final PreparedStatement failing = mock(PreparedStatement.class);
        when(failing.executeQuery()).thenThrow(new SQLException("access denied", "28000"));
        // The second chunk needs the full budget: it fails once with a retriable error, then succeeds
        final PreparedStatement retried = mock(PreparedStatement.class);
        when(retried.executeQuery()).thenThrow(retriableError());
        final ResultSet resultSetForSucceeding = mockResultSet(new Object[][]{ ROW_1 }, null, null);
        final PreparedStatement succeeding = mock(PreparedStatement.class);
        when(succeeding.executeQuery()).thenReturn(resultSetForSucceeding);
        when(sqlConnection.prepareStatement(anyString())).thenReturn(failing, retried, succeeding);

        assertThatThrownBy(chunkedCallable(source, keyedSingleChunk(table))::call)
                .isInstanceOf(ConnectException.class);
        verify(sqlConnection, times(1)).prepareStatement(anyString());

        // Succeeds only because the non-retriable failure above left the budget untouched
        final Table table2 = keyedTable("table2");
        chunkedCallable(source, keyedSingleChunk(table2)).call();
        assertThat(emittedRows).containsExactly(ROW_1);
    }

    @Test
    @FixFor("debezium/dbz#2297")
    public void interruptionAbortsWithoutRetrying() throws Exception {
        final TestSnapshotSource source = newSource(2);
        final Table table = keyedTable();

        // The connector stops while the chunk is being read
        when(sourceContext.isRunning()).thenReturn(true, true, false);
        final ResultSet resultSetForStatement = mockResultSet(new Object[][]{ ROW_1, ROW_2 }, null, null);
        final PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenReturn(resultSetForStatement);
        when(sqlConnection.prepareStatement(anyString())).thenReturn(statement);

        assertThatThrownBy(chunkedCallable(source, keyedSingleChunk(table))::call)
                .isInstanceOf(InterruptedException.class);

        verify(sqlConnection, times(1)).prepareStatement(anyString());
        assertThat(jdbcConnection.rollbackCount).isZero();
        verify(notificationService.initialSnapshotNotificationService(), never())
                .notifyTableSnapshotRetry(any(), any(), anyString(), anyInt(), anyInt());
    }

    @Test
    @FixFor("debezium/dbz#2297")
    public void keylessTableFallsBackToReReadAcceptingDuplicates() throws Exception {
        final TestSnapshotSource source = newSource(2);
        final Table table = keylessTable();
        // A keyless table is snapshotted as a single unbounded, unordered chunk
        final SnapshotChunk chunk = new SnapshotChunk(table.id(), table, null, null, 0, 1, 1, 1, BASE_SELECT, OptionalLong.of(2));

        final ResultSet resultSetForStatement1 = mockResultSet(new Object[][]{ ROW_1, ROW_2 }, 3, retriableError());
        final PreparedStatement statement1 = mock(PreparedStatement.class);
        when(statement1.executeQuery()).thenReturn(resultSetForStatement1);
        final ResultSet resultSetForStatement2 = mockResultSet(new Object[][]{ ROW_1, ROW_2 }, null, null);
        final PreparedStatement statement2 = mock(PreparedStatement.class);
        when(statement2.executeQuery()).thenReturn(resultSetForStatement2);
        when(sqlConnection.prepareStatement(anyString())).thenReturn(statement1, statement2);

        chunkedCallable(source, chunk).call();

        // With no key to resume from, the retry re-reads the whole chunk: ROW_1 is duplicated, but the
        // FIRST/LAST markers are still emitted exactly once
        assertThat(emittedRows).containsExactly(ROW_1, ROW_1, ROW_2);
        assertThat(emittedMarkers).containsExactly(SnapshotRecord.FIRST, SnapshotRecord.TRUE, SnapshotRecord.LAST);

        // Both attempts use the unmodified base select: no boundaries, no ORDER BY
        final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(sqlConnection, times(2)).prepareStatement(sqlCaptor.capture());
        assertThat(sqlCaptor.getAllValues()).containsExactly(BASE_SELECT, BASE_SELECT);
    }

    @Test
    @FixFor("debezium/dbz#2297")
    public void singleThreadedTableRetryResumesFromLastEmittedKey() throws Exception {
        final TestSnapshotSource source = newSource(2);
        final Table table = keyedTable();

        // Attempt 1 runs the plain statement path and emits ROW_1 before failing on the next row
        final ResultSet resultSetForStatement1 = mockResultSet(new Object[][]{ ROW_1, ROW_2 }, 3, retriableError());
        final Statement statement1 = mock(Statement.class);
        when(statement1.executeQuery(anyString())).thenReturn(resultSetForStatement1);
        when(sqlConnection.createStatement()).thenReturn(statement1);
        // Attempt 2 resumes after the last emitted key on the prepared statement path
        final ResultSet resultSetForStatement2 = mockResultSet(new Object[][]{ ROW_2, ROW_3 }, null, null);
        final PreparedStatement statement2 = mock(PreparedStatement.class);
        when(statement2.executeQuery()).thenReturn(resultSetForStatement2);
        when(sqlConnection.prepareStatement(anyString())).thenReturn(statement2);

        tableCallable(source, table, BASE_SELECT).call();

        assertThat(emittedRows).containsExactly(ROW_1, ROW_2, ROW_3);
        assertThat(emittedMarkers).containsExactly(SnapshotRecord.FIRST, SnapshotRecord.TRUE, SnapshotRecord.LAST);

        // With retry opted in, the initial scan is ordered by the key so that the resume position is
        // well-defined, and the retry adds the exclusive lower bound
        final ArgumentCaptor<String> selectCaptor = ArgumentCaptor.forClass(String.class);
        verify(statement1).executeQuery(selectCaptor.capture());
        assertThat(selectCaptor.getValue()).isEqualTo("SELECT * FROM \"s1\".\"table1\" ORDER BY \"pk1\"");
        final ArgumentCaptor<String> resumeCaptor = ArgumentCaptor.forClass(String.class);
        verify(sqlConnection).prepareStatement(resumeCaptor.capture());
        assertThat(resumeCaptor.getValue()).isEqualTo("SELECT * FROM \"s1\".\"table1\" WHERE (\"pk1\" > ?) ORDER BY \"pk1\"");
        verify(statement2).setObject(1, 1);
    }

    @Test
    @FixFor("debezium/dbz#2297")
    public void singleThreadedTableWithoutRetryKeepsUnorderedSelect() throws Exception {
        final TestSnapshotSource source = newSource(0);
        final Table table = keyedTable();

        final ResultSet resultSetForStatement = mockResultSet(new Object[][]{ ROW_1, ROW_2 }, null, null);
        final Statement statement = mock(Statement.class);
        when(statement.executeQuery(anyString())).thenReturn(resultSetForStatement);
        when(sqlConnection.createStatement()).thenReturn(statement);

        tableCallable(source, table, BASE_SELECT).call();

        assertThat(emittedRows).containsExactly(ROW_1, ROW_2);
        // Without retry opted in, the select is passed through unchanged - no ORDER BY is added
        verify(statement).executeQuery(BASE_SELECT);
    }

    @Test
    @FixFor("debezium/dbz#2297")
    public void retryIsDisabledByDefault() {
        final RelationalDatabaseConnectorConfig config = buildConfig(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "test")
                .build());

        assertThat(config.snapshotRetryMax()).isZero();
        assertThat(config.snapshotRetryDelay().toMillis()).isEqualTo(10_000L);
    }

    private Callable<Void> chunkedCallable(TestSnapshotSource source, SnapshotChunk chunk) {
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<Partition, OffsetContext> snapshotContext = snapshotContext(chunk.getTableId());
        final Map<TableId, TableChunkProgress> progressMap = Map.of(chunk.getTableId(), new TableChunkProgress(chunk.getTableId(), chunk.getTotalChunks()));
        final SnapshotProgress snapshotProgress = new SnapshotProgress(chunk.getTableCount());
        return source.createDataEventsForChunkedTableCallable(sourceContext, snapshotContext, snapshotReceiver, chunk,
                progressMap, snapshotProgress, pool(), offsets());
    }

    private Callable<Void> tableCallable(TestSnapshotSource source, Table table, String selectStatement) {
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<Partition, OffsetContext> snapshotContext = snapshotContext(table.id());
        return source.createDataEventsForTableCallable(sourceContext, snapshotContext, snapshotReceiver, table, true, true,
                1, 1, selectStatement, OptionalLong.of(3), Set.of(table.id()), pool(), offsets());
    }

    private RelationalSnapshotChangeEventSource.RelationalSnapshotContext<Partition, OffsetContext> snapshotContext(TableId tableId) {
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<Partition, OffsetContext> snapshotContext = new RelationalSnapshotChangeEventSource.RelationalSnapshotContext<>(
                partition, "s1", false);
        snapshotContext.offset = offset;
        snapshotContext.capturedTables = Set.of(tableId);
        return snapshotContext;
    }

    private Queue<JdbcConnection> pool() {
        final Queue<JdbcConnection> pool = new ConcurrentLinkedQueue<>();
        pool.add(jdbcConnection);
        return pool;
    }

    private Queue<OffsetContext> offsets() {
        final Queue<OffsetContext> offsets = new ConcurrentLinkedQueue<>();
        offsets.add(offset);
        return offsets;
    }

    private SnapshotChunk keyedSingleChunk(Table table) {
        // A single-chunk keyed table as created by the chunk boundary calculator: no lower bound, the
        // maximum key as the (inclusive, since it is the last chunk) upper bound
        return new SnapshotChunk(table.id(), table, null, new Object[]{ 10 }, 0, 1, 1, 1, BASE_SELECT, OptionalLong.of(3));
    }

    private Table keyedTable() {
        return keyedTable("table1");
    }

    private Table keyedTable(String tableName) {
        return Table.editor().tableId(new TableId(null, "s1", tableName))
                .addColumn(Column.editor().name("pk1").optional(false).create())
                .addColumn(Column.editor().name("val1").create())
                .setPrimaryKeyNames("pk1")
                .create();
    }

    private Table keylessTable() {
        return Table.editor().tableId(new TableId(null, "s1", "table1"))
                .addColumn(Column.editor().name("pk1").optional(false).create())
                .addColumn(Column.editor().name("val1").create())
                .create();
    }

    private SQLException retriableError() {
        return new SQLException("connection reset", RETRIABLE_SQL_STATE);
    }

    /**
     * Mocks a result set over the given rows (pk1, val1). When {@code failOnNextCall} is non-null, the
     * given failure is thrown by that (1-based) invocation of {@link ResultSet#next()} instead of
     * advancing; otherwise {@code next()} returns {@code false} after the last row.
     */
    private ResultSet mockResultSet(Object[][] rows, Integer failOnNextCall, SQLException failure) throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        final ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnName(1)).thenReturn("pk1");
        when(metaData.getColumnName(2)).thenReturn("val1");
        when(resultSet.getMetaData()).thenReturn(metaData);

        OngoingStubbing<Boolean> nextStubbing = when(resultSet.next());
        final int successfulNextCalls = failOnNextCall != null ? failOnNextCall - 1 : rows.length;
        for (int i = 0; i < successfulNextCalls; i++) {
            nextStubbing = nextStubbing.thenReturn(true);
        }
        if (failOnNextCall != null) {
            nextStubbing.thenThrow(failure);
        }
        else {
            nextStubbing.thenReturn(false);
        }

        OngoingStubbing<Object> pkStubbing = when(resultSet.getObject(1));
        for (Object[] row : rows) {
            pkStubbing = pkStubbing.thenReturn(row[0]);
        }
        OngoingStubbing<Object> valueStubbing = when(resultSet.getObject(2));
        for (Object[] row : rows) {
            valueStubbing = valueStubbing.thenReturn(row[1]);
        }
        return resultSet;
    }

    private RelationalDatabaseConnectorConfig config(int maxRetries) {
        return buildConfig(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "test")
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_RETRY_MAX, maxRetries)
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_RETRY_DELAY_MS, 1)
                .build());
    }

    private RelationalDatabaseConnectorConfig buildConfig(Configuration configuration) {
        return new RelationalDatabaseConnectorConfig(configuration, null, null,
                0, ColumnFilterMode.CATALOG, true) {
            @Override
            protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
                return null;
            }

            @Override
            public String getContextName() {
                return "TestConnector";
            }

            @Override
            public String getConnectorName() {
                return "test";
            }

            @Override
            public EnumeratedValue getSnapshotMode() {
                return null;
            }

            @Override
            public Optional<EnumeratedValue> getSnapshotLockingMode() {
                return Optional.empty();
            }
        };
    }

    /**
     * A {@link JdbcConnection} whose JDBC-level calls are served by a mocked {@link Connection}, and which
     * counts the rollbacks/reconnects issued by the retry's connection recovery.
     */
    private static class FakeJdbcConnection extends JdbcConnection {

        private final Connection sqlConnection;
        private int rollbackCount;
        private int reconnectCount;

        FakeJdbcConnection(JdbcConfiguration config, Connection sqlConnection) {
            super(config, c -> null, "\"", "\"");
            this.sqlConnection = sqlConnection;
        }

        @Override
        public synchronized Connection connection() {
            return sqlConnection;
        }

        @Override
        public synchronized Connection connection(boolean executeOnConnect) {
            return sqlConnection;
        }

        @Override
        public synchronized boolean isValid() {
            return true;
        }

        @Override
        public void reconnect() {
            reconnectCount++;
        }

        @Override
        public synchronized JdbcConnection rollback() {
            rollbackCount++;
            return this;
        }

        @Override
        public Optional<Instant> getCurrentTimestamp() {
            return Optional.of(Instant.EPOCH);
        }

        @Override
        public Statement readTableStatement(CommonConnectorConfig connectorConfig, OptionalLong tableSize) throws SQLException {
            return sqlConnection.createStatement();
        }

        @Override
        public PreparedStatement readTablePreparedStatement(CommonConnectorConfig connectorConfig, String sql, OptionalLong tableSize) throws SQLException {
            return sqlConnection.prepareStatement(sql);
        }
    }

    /**
     * Minimal concrete snapshot source: classifies SQLState {@value #RETRIABLE_SQL_STATE} as retriable and
     * records each emitted row together with the snapshot marker in effect when it was dispatched.
     */
    private class TestSnapshotSource extends RelationalSnapshotChangeEventSource<Partition, OffsetContext> {

        @SuppressWarnings("unchecked")
        TestSnapshotSource(RelationalDatabaseConnectorConfig connectorConfig,
                           MainConnectionProvidingConnectionFactory<JdbcConnection> connectionFactory,
                           NotificationService<Partition, OffsetContext> notificationService) {
            super(connectorConfig, connectionFactory, null, mock(EventDispatcher.class), Clock.SYSTEM,
                    mock(SnapshotProgressListener.class), notificationService, mock(SnapshotterService.class));
        }

        @Override
        protected boolean isSnapshotErrorRetriable(SQLException exception) {
            return RETRIABLE_SQL_STATE.equals(exception.getSQLState());
        }

        @Override
        protected ChangeRecordEmitter<Partition> getChangeRecordEmitter(Partition partition, OffsetContext offset, TableId tableId,
                                                                        Object[] row, Instant timestamp) {
            emittedRows.add(row.clone());
            emittedMarkers.add(currentMarker.get());
            return null;
        }

        @Override
        protected Set<TableId> getAllTableIds(RelationalSnapshotContext<Partition, OffsetContext> snapshotContext) {
            return Set.of();
        }

        @Override
        protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                                   RelationalSnapshotContext<Partition, OffsetContext> snapshotContext) {
        }

        @Override
        protected void determineSnapshotOffset(RelationalSnapshotContext<Partition, OffsetContext> snapshotContext, OffsetContext previousOffset) {
        }

        @Override
        protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                          RelationalSnapshotContext<Partition, OffsetContext> snapshotContext, OffsetContext offsetContext,
                                          SnapshottingTask snapshottingTask) {
        }

        @Override
        protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<Partition, OffsetContext> snapshotContext) {
        }

        @Override
        protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<Partition, OffsetContext> snapshotContext, Table table) {
            return null;
        }

        @Override
        protected OffsetContext copyOffset(RelationalSnapshotContext<Partition, OffsetContext> snapshotContext) {
            return snapshotContext.offset;
        }

        @Override
        protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<Partition, OffsetContext> snapshotContext, TableId tableId,
                                                     List<String> columns) {
            return Optional.empty();
        }

        @Override
        protected SnapshotContext<Partition, OffsetContext> prepare(Partition partition, boolean onDemand) {
            return new RelationalSnapshotContext<>(partition, "s1", onDemand);
        }
    }

    private TestSnapshotSource newSource(int maxRetries) {
        return new TestSnapshotSource(config(maxRetries), connectionFactory(), notificationService);
    }

    @SuppressWarnings("unchecked")
    private MainConnectionProvidingConnectionFactory<JdbcConnection> connectionFactory() {
        final MainConnectionProvidingConnectionFactory<JdbcConnection> factory = mock(MainConnectionProvidingConnectionFactory.class);
        when(factory.mainConnection()).thenReturn(jdbcConnection);
        return factory;
    }
}
