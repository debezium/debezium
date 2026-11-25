/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.data.Envelope.Operation;
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
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Unit tests for {@link RelationalSnapshotChangeEventSource}.
 */
public class RelationalSnapshotChangeEventSourceTest {

    private static final int CONFIGURED_FETCH_SIZE = 1234;
    private static final String BASE_SELECT = "SELECT * FROM \"s1\".\"t1\"";

    /**
     * The chunked (parallel) initial snapshot must apply the configured {@code snapshot.fetch.size}
     * to the per-chunk {@link PreparedStatement}, matching the behavior of the legacy snapshot path
     * ({@link JdbcConnection#readTableStatement}) and the incremental snapshot chunk queries
     * ({@link JdbcConnection#readTablePreparedStatement}).
     */
    @Test
    @FixFor("debezium/dbz#2483")
    public void shouldApplyConfiguredSnapshotFetchSizeToChunkStatement() throws Exception {
        final Connection connection = mock(Connection.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        // Fail fast on execution so the test stops right after statement creation; all we
        // assert on is how the statement was configured.
        when(statement.executeQuery()).thenThrow(new SQLException("stop after statement creation"));

        final JdbcConnection jdbcConnection = new JdbcConnection(
                JdbcConfiguration.adapt(Configuration.create().build()), config -> connection, "\"", "\"");

        final TestRelationalSnapshotSource source = new TestRelationalSnapshotSource(config(Configuration.empty()), jdbcConnection);

        final TableId tableId = new TableId("db1", "s1", "t1");
        final Table table = Table.editor()
                .tableId(tableId)
                .addColumn(Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).create())
                .setPrimaryKeyNames("id")
                .create();
        final SnapshotChunk chunk = new SnapshotChunk(tableId, table, null, null, 0, 1, 1, 1,
                BASE_SELECT, OptionalLong.of(1_000_000L));

        final ChangeEventSourceContext sourceContext = mock(ChangeEventSourceContext.class);
        when(sourceContext.isRunning()).thenReturn(true);

        @SuppressWarnings("unchecked")
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<Partition, OffsetContext> snapshotContext = mock(
                RelationalSnapshotChangeEventSource.RelationalSnapshotContext.class);
        @SuppressWarnings("unchecked")
        final SnapshotReceiver<Partition> snapshotReceiver = mock(SnapshotReceiver.class);
        final Map<TableId, TableChunkProgress> progressMap = Map.of(tableId, new TableChunkProgress(tableId, 1));
        final SnapshotProgress snapshotProgress = new SnapshotProgress(1);

        assertThatThrownBy(() -> source.doCreateDataEventsForChunk(sourceContext, snapshotContext, null,
                snapshotReceiver, chunk, progressMap, snapshotProgress, jdbcConnection))
                .isInstanceOf(SQLException.class);

        // The chunk statement must carry the configured snapshot.fetch.size, like the legacy
        // and incremental snapshot paths do.
        verify(statement).setFetchSize(CONFIGURED_FETCH_SIZE);
    }

    @Test
    @FixFor("debezium/dbz#1886")
    public void shouldCompleteSnapshotWhenConnectionIsClosedAndReplacedBeforeCleanup() throws Exception {
        final AtomicBoolean originalClosed = new AtomicBoolean();
        final AtomicBoolean replacementClosed = new AtomicBoolean();
        final AtomicInteger rollbackCalls = new AtomicInteger();
        final AtomicInteger connectionFactoryCalls = new AtomicInteger();
        final Connection originalConnection = connection(originalClosed, rollbackCalls, false);
        final Connection replacementConnection = connection(replacementClosed, rollbackCalls, false);
        final JdbcConnection jdbcConnection = new JdbcConnection(JdbcConfiguration.empty(),
                config -> connectionFactoryCalls.getAndIncrement() == 0 ? originalConnection : replacementConnection, "\"", "\"");
        jdbcConnection.connect();

        final TestRelationalSnapshotSource source = new TestRelationalSnapshotSource(config(Configuration.empty()), jdbcConnection,
                originalConnection);
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<Partition, OffsetContext> snapshotContext = snapshotContext();
        final SnapshottingTask snapshottingTask = new SnapshottingTask(false, true, List.of(), Map.of(), false);

        final var result = source.doExecute(null, null, snapshotContext, snapshottingTask);

        assertThat(result.isCompleted()).isTrue();
        assertThat(originalClosed.get()).isTrue();
        assertThat(rollbackCalls.get()).isZero();
        assertThat(connectionFactoryCalls.get()).isEqualTo(2);
        assertThat(jdbcConnection.connection()).isSameAs(replacementConnection);
        assertThat(replacementClosed.get()).isFalse();

        jdbcConnection.close();

        assertThat(replacementClosed.get()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1886")
    public void shouldSurfaceRollbackFailureForOpenConnection() throws Exception {
        final AtomicBoolean connectionClosed = new AtomicBoolean();
        final AtomicInteger rollbackCalls = new AtomicInteger();
        final Connection connection = connection(connectionClosed, rollbackCalls, true);
        final JdbcConnection jdbcConnection = new JdbcConnection(JdbcConfiguration.empty(), config -> connection, "\"", "\"");
        jdbcConnection.connect();
        final TestRelationalSnapshotSource source = new TestRelationalSnapshotSource(config(Configuration.empty()), jdbcConnection, null);

        assertThatThrownBy(() -> source.doExecute(null, null, snapshotContext(),
                new SnapshottingTask(false, true, List.of(), Map.of(), false)))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(SQLException.class);

        assertThat(rollbackCalls.get()).isEqualTo(1);
        jdbcConnection.close();
    }

    private static RelationalSnapshotChangeEventSource.RelationalSnapshotContext<Partition, OffsetContext> snapshotContext() {
        final Partition partition = Map::of;
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<Partition, OffsetContext> snapshotContext = new RelationalSnapshotChangeEventSource.RelationalSnapshotContext<>(
                partition, "db1", true);
        snapshotContext.offset = new TestOffsetContext();
        return snapshotContext;
    }

    private static Connection connection(AtomicBoolean closed, AtomicInteger rollbackCalls, boolean failRollback) {
        return (Connection) Proxy.newProxyInstance(RelationalSnapshotChangeEventSourceTest.class.getClassLoader(),
                new Class<?>[]{ Connection.class }, (proxy, method, args) -> switch (method.getName()) {
                    case "isClosed" -> closed.get();
                    case "isValid" -> !closed.get();
                    case "setAutoCommit" -> null;
                    case "close" -> {
                        closed.set(true);
                        yield null;
                    }
                    case "rollback" -> {
                        rollbackCalls.incrementAndGet();
                        if (closed.get() || failRollback) {
                            throw new SQLException(closed.get() ? "connection is closed" : "rollback failed");
                        }
                        yield null;
                    }
                    case "toString" -> "snapshot test connection";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    @SuppressWarnings("unchecked")
    private static EventDispatcher<Partition, TableId> eventDispatcher() {
        final EventDispatcher<Partition, TableId> dispatcher = mock(EventDispatcher.class);
        when(dispatcher.getSnapshotChangeEventReceiver()).thenReturn(new SnapshotReceiver<Partition>() {
            @Override
            public void changeRecord(Partition partition, DataCollectionSchema schema, Operation operation, Object key,
                                     Struct value, OffsetContext offset, ConnectHeaders headers) {
            }

            @Override
            public void completeSnapshot() {
            }
        });
        return dispatcher;
    }

    private static class TestOffsetContext implements OffsetContext {
        @Override
        public Map<String, ?> getOffset() {
            return Map.of();
        }

        @Override
        public Schema getSourceInfoSchema() {
            return null;
        }

        @Override
        public Struct getSourceInfo() {
            return null;
        }

        @Override
        public boolean isInitialSnapshotRunning() {
            return false;
        }

        @Override
        public void markSnapshotRecord(SnapshotRecord record) {
        }

        @Override
        public void preSnapshotStart(boolean onDemand) {
        }

        @Override
        public void preSnapshotCompletion() {
        }

        @Override
        public void postSnapshotCompletion() {
        }

        @Override
        public void event(DataCollectionId collectionId, Instant timestamp) {
        }

        @Override
        public TransactionContext getTransactionContext() {
            return null;
        }
    }

    @Test
    @FixFor("debezium/dbz#488")
    public void shouldSelectKeyColumnsExcludedByColumnFilter() {
        final RelationalDatabaseConnectorConfig config = config(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST, ".*\\.(id|secret)$")
                .build());
        final JdbcConnection jdbcConnection = new JdbcConnection(
                JdbcConfiguration.adapt(Configuration.create().build()), c -> null, "\"", "\"");
        final TestRelationalSnapshotSource source = new TestRelationalSnapshotSource(config, jdbcConnection);

        final Table table = Table.editor()
                .tableId(new TableId("db1", "s1", "t1"))
                .addColumn(Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).create())
                .addColumn(Column.editor().name("name").type("VARCHAR").jdbcType(Types.VARCHAR).create())
                .addColumn(Column.editor().name("secret").type("VARCHAR").jdbcType(Types.VARCHAR).create())
                .setPrimaryKeyNames("id")
                .create();

        assertThat(source.getPreparedColumnNames(null, table)).containsExactly("\"id\"", "\"name\"");
    }

    private static RelationalDatabaseConnectorConfig config(Configuration additionalConfig) {
        final Configuration configuration = Configuration.create()
                .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "core")
                .with(CommonConnectorConfig.SNAPSHOT_FETCH_SIZE, CONFIGURED_FETCH_SIZE)
                .with(additionalConfig)
                .build();
        return new RelationalDatabaseConnectorConfig(configuration, null, null, 0, ColumnFilterMode.CATALOG, true) {
            @Override
            protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
                return null;
            }

            @Override
            public String getContextName() {
                return null;
            }

            @Override
            public String getConnectorName() {
                return null;
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

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldCreateSnapshotTablesWithDefaultMetadataFactory() {
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<Partition, OffsetContext> snapshotContext = new RelationalSnapshotChangeEventSource.RelationalSnapshotContext<>(
                Map::of, "db1", false);

        final Table table = snapshotContext.tables.editOrCreateTable(new TableId("db1", "s1", "t1")).create();

        assertThat(table).isExactlyInstanceOf(TableImpl.class);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldCreateSnapshotTablesWithConfiguredMetadataFactory() {
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<Partition, OffsetContext> snapshotContext = new RelationalSnapshotChangeEventSource.RelationalSnapshotContext<>(
                Map::of, "db1", false, ExtendedMetadata.ExtendedRelationalMetadataFactory.INSTANCE);

        final Table table = snapshotContext.tables.editOrCreateTable(new TableId("db1", "s1", "t1")).create();

        assertThat(table).isInstanceOf(ExtendedMetadata.ExtendedTable.class);
    }

    /**
     * Minimal concrete snapshot source; only {@code doCreateDataEventsForChunk} is exercised.
     */
    private static class TestRelationalSnapshotSource extends RelationalSnapshotChangeEventSource<Partition, OffsetContext> {

        private final JdbcConnection jdbcConnection;
        private final Connection connectionToCloseAfterSnapshot;

        @SuppressWarnings("unchecked")
        TestRelationalSnapshotSource(RelationalDatabaseConnectorConfig connectorConfig, JdbcConnection jdbcConnection) {
            this(connectorConfig, jdbcConnection, null);
        }

        @SuppressWarnings("unchecked")
        TestRelationalSnapshotSource(RelationalDatabaseConnectorConfig connectorConfig, JdbcConnection jdbcConnection,
                                     Connection connectionToCloseAfterSnapshot) {
            super(connectorConfig, connectionFactory(jdbcConnection), null, eventDispatcher(), Clock.SYSTEM,
                    mock(SnapshotProgressListener.class), mock(NotificationService.class, RETURNS_DEEP_STUBS),
                    mock(SnapshotterService.class));
            this.jdbcConnection = jdbcConnection;
            this.connectionToCloseAfterSnapshot = connectionToCloseAfterSnapshot;
        }

        @SuppressWarnings("unchecked")
        private static MainConnectionProvidingConnectionFactory<JdbcConnection> connectionFactory(JdbcConnection jdbcConnection) {
            final MainConnectionProvidingConnectionFactory<JdbcConnection> connectionFactory = mock(MainConnectionProvidingConnectionFactory.class);
            when(connectionFactory.mainConnection()).thenReturn(jdbcConnection);
            return connectionFactory;
        }

        @Override
        protected Instant getSnapshotSourceTimestamp(JdbcConnection jdbcConnection, OffsetContext offset, TableId tableId) {
            return Instant.EPOCH;
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
                                          RelationalSnapshotContext<Partition, OffsetContext> snapshotContext,
                                          OffsetContext offsetContext, SnapshottingTask snapshottingTask) {
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
            return null;
        }

        @Override
        protected void postSnapshot() {
            if (connectionToCloseAfterSnapshot != null) {
                try {
                    connectionToCloseAfterSnapshot.close();
                    jdbcConnection.reconnect();
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<Partition, OffsetContext> snapshotContext,
                                                     TableId tableId, List<String> columns) {
            return Optional.empty();
        }

        @Override
        protected SnapshotContext<Partition, OffsetContext> prepare(Partition partition, boolean onDemand) {
            return null;
        }
    }
}
