/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
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
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
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

        final TestRelationalSnapshotSource source = new TestRelationalSnapshotSource(config(), jdbcConnection);

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

    private static RelationalDatabaseConnectorConfig config() {
        final Configuration configuration = Configuration.create()
                .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "core")
                .with(CommonConnectorConfig.SNAPSHOT_FETCH_SIZE, CONFIGURED_FETCH_SIZE)
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

    /**
     * Minimal concrete snapshot source; only {@code doCreateDataEventsForChunk} is exercised.
     */
    private static class TestRelationalSnapshotSource extends RelationalSnapshotChangeEventSource<Partition, OffsetContext> {

        @SuppressWarnings("unchecked")
        TestRelationalSnapshotSource(RelationalDatabaseConnectorConfig connectorConfig, JdbcConnection jdbcConnection) {
            super(connectorConfig, connectionFactory(jdbcConnection), null, mock(EventDispatcher.class), Clock.SYSTEM,
                    mock(SnapshotProgressListener.class), mock(NotificationService.class, RETURNS_DEEP_STUBS),
                    mock(SnapshotterService.class));
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
