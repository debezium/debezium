/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.ColumnUtils;
import io.debezium.util.LoggingContext;

/**
 * Verifies how {@link AbstractIncrementalSnapshotChangeEventSource#readChunk} reacts when reading a
 * chunk fails with a JDBC error. A non-transient connection error (the server closed the connection)
 * must lead to the stale connection being discarded so that a fresh one is opened on the next chunk
 * read, rather than the connector getting stuck retrying a broken connection.
 */
@ExtendWith(MockitoExtension.class)
public class AbstractIncrementalSnapshotChangeEventSourceTest {

    interface TestPartition extends Partition {
    }

    private JdbcConnection jdbcConnection;
    private OffsetContext offsetContext;
    private SignalBasedIncrementalSnapshotChangeEventSource<TestPartition, TableId> source;
    private SnapshotProgressListener<TestPartition> progressListener;
    private NotificationService<TestPartition, OffsetContext> notificationService;
    private SignalBasedIncrementalSnapshotContext<TableId> context;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        jdbcConnection = mock(JdbcConnection.class);
        progressListener = mock(SnapshotProgressListener.class);
        notificationService = mock(NotificationService.class, RETURNS_DEEP_STUBS);
        source = new SignalBasedIncrementalSnapshotChangeEventSource<>(config(), jdbcConnection, null, null, null, progressListener, null,
                notificationService);

        context = new SignalBasedIncrementalSnapshotContext<>();
        context.addDataCollectionNamesToSnapshot("signal-1", List.of("public.a", "public.b"), List.of(), "");

        offsetContext = mock(OffsetContext.class);
        when(offsetContext.getIncrementalSnapshotContext()).thenAnswer(invocation -> context);
    }

    @Test
    @FixFor("dbz#2275")
    public void shouldCloseConnectionWhenChunkReadFailsWithNonTransientConnectionError() throws Exception {
        // The server has closed the connection: the first JDBC call while reading the chunk fails
        // with a non-transient connection error.
        when(jdbcConnection.commit()).thenThrow(new SQLNonTransientConnectionException("connection closed by server"));

        source.readChunk(null, offsetContext);

        // The dead connection must be closed so it is re-opened on the next chunk read.
        verify(jdbcConnection).close();
        verify(jdbcConnection, never()).rollback();
    }

    @Test
    @FixFor("dbz#2275")
    public void shouldNotCloseConnectionWhenChunkReadFailsWithOtherSqlError() throws Exception {
        // A generic (potentially transient) SQL error is not a broken connection and must not cause
        // the connection to be discarded.
        when(jdbcConnection.commit()).thenThrow(new SQLException("transient failure"));

        source.readChunk(null, offsetContext);

        verify(jdbcConnection, never()).close();
        verify(jdbcConnection, never()).rollback();
    }

    @Test
    @FixFor("debezium/dbz#2604")
    @SuppressWarnings("unchecked")
    public void shouldPreserveSharedConnectionWhenSkippingInvalidSurrogateKey() throws Exception {
        // Db2 reads signals while a streaming cursor is open on this connection. Skipping an invalid
        // surrogate key must not roll back the shared transaction and close that cursor.
        final var schema = mock(RelationalDatabaseSchema.class);
        final var tableId = TableId.parse("public.a");
        final var table = Table.editor().tableId(tableId)
                .addColumn(Column.editor().name("pk").jdbcType(Types.INTEGER).type("INTEGER").position(1).optional(false).create())
                .setPrimaryKeyNames("pk").create();
        when(schema.tableFor(tableId)).thenReturn(table);

        final var chunkQueryBuilder = new DefaultChunkQueryBuilder<TableId>(config(), jdbcConnection);
        doReturn(chunkQueryBuilder).when(jdbcConnection).chunkQueryBuilder(any());
        source = new SignalBasedIncrementalSnapshotChangeEventSource<>(config(), jdbcConnection, null, schema, null, progressListener, null,
                notificationService);
        context = new SignalBasedIncrementalSnapshotContext<>();
        context.addDataCollectionNamesToSnapshot("signal-1", List.of("public.a", "public.b"), List.of(), "missing_column");

        source.readChunk(null, offsetContext);

        verify(schema).tableFor(tableId);
        verify(jdbcConnection, never()).queryAndMap(anyString(), any(JdbcConnection.ResultSetMapper.class));
        verify(jdbcConnection, never()).rollback();
        verify(jdbcConnection, never()).close();
        assertThat(context.currentDataCollectionId().getId()).isEqualTo(TableId.parse("public.b"));
        verify(progressListener, never()).snapshotCompleted(null);
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2604")
    @MethodSource("failuresOutsideSchemaMismatchRecovery")
    public void shouldSkipTableWithoutRollingBackSharedConnectionOutsideRecovery(Exception readFailure, boolean recoverySupported, boolean schemaChangesEnabled)
            throws Exception {
        source = recoverySource(recoverySupported, schemaChangesEnabled);
        when(jdbcConnection.commit()).thenThrow(readFailure);
        final var queue = errorQueue();
        try {
            final var errorHandler = new ErrorHandler(SourceConnector.class, config(), queue, null);
            source.setErrorHandler(errorHandler);

            source.readChunk(null, offsetContext);

            verify(jdbcConnection, never()).rollback();
            verify(jdbcConnection, never()).close();
            assertThat(context.currentDataCollectionId().getId()).isEqualTo(TableId.parse("public.b"));
            verify(progressListener, never()).snapshotCompleted(null);
            assertThat(errorHandler.getProducerThrowable()).isNull();
            assertThat(queue.poll()).isEmpty();
        }
        finally {
            queue.close();
        }
    }

    private static Stream<Arguments> failuresOutsideSchemaMismatchRecovery() {
        return Stream.of(
                Arguments.of(new SQLException("chunk failed"), false, true),
                Arguments.of(new IllegalArgumentException("chunk failed"), false, true),
                Arguments.of(new SQLException("chunk failed"), true, true),
                Arguments.of(new IllegalArgumentException("chunk failed"), true, true),
                Arguments.of(new ColumnUtils.SchemaMismatchException("schema mismatch"), false, true),
                Arguments.of(new ColumnUtils.SchemaMismatchException("schema mismatch"), true, false),
                Arguments.of(new ColumnUtils.SchemaMismatchException("schema mismatch"), false, false));
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2604")
    @ValueSource(booleans = { false, true })
    public void shouldReportRollbackFailureDuringSchemaMismatchRecovery(boolean closeFails) throws Exception {
        source = recoverySource(true, true);
        context.sendEvent(new Object[]{ 2 });
        context.nextChunkPosition(new Object[]{ 2 });
        final var readFailure = new ColumnUtils.SchemaMismatchException("schema mismatch");
        final var rollbackFailure = new SQLException("rollback failed");
        final var closeFailure = new SQLException("close failed");
        when(jdbcConnection.commit()).thenThrow(readFailure);
        when(jdbcConnection.rollback()).thenThrow(rollbackFailure);
        if (closeFails) {
            doThrow(closeFailure).when(jdbcConnection).close();
        }
        final var queue = errorQueue();
        try {
            final var errorHandler = new ErrorHandler(SourceConnector.class, config(), queue, null);
            source.setErrorHandler(errorHandler);

            assertThatThrownBy(() -> source.readChunk(null, offsetContext))
                    .hasMessageContaining("Could not roll back")
                    .hasCause(rollbackFailure);

            verify(jdbcConnection).close();
            assertThat(context.currentDataCollectionId().getId()).isEqualTo(TableId.parse("public.a"));
            assertThat(context.chunkEndPosititon()).containsExactly(2);
            verify(progressListener, never()).snapshotCompleted(null);
            // A failure while discarding the connection must not hide why recovery failed or
            // the original read error needed to diagnose the incomplete snapshot chunk.
            if (closeFails) {
                assertThat(rollbackFailure.getSuppressed()).containsExactly(readFailure, closeFailure);
            }
            else {
                assertThat(rollbackFailure.getSuppressed()).containsExactly(readFailure);
            }
            assertThat(errorHandler.getProducerThrowable()).hasCause(rollbackFailure);
            assertThatThrownBy(queue::poll).isInstanceOf(ConnectException.class);
        }
        finally {
            queue.close();
        }
    }

    private ChangeEventQueue<DataChangeEvent> errorQueue() {
        return new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(10).maxQueueSize(20).pollInterval(Duration.ofMillis(10))
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "recovery", "test"))
                .build();
    }

    private SignalBasedIncrementalSnapshotChangeEventSource<TestPartition, TableId> recoverySource(boolean recoverySupported, boolean schemaChangesEnabled) {
        return new SignalBasedIncrementalSnapshotChangeEventSource<>(config(schemaChangesEnabled), jdbcConnection, null, null, null, progressListener, null,
                notificationService) {
            @Override
            protected boolean supportsSchemaMismatchRecovery() {
                return recoverySupported;
            }
        };
    }

    private RelationalDatabaseConnectorConfig config() {
        return config(false);
    }

    private RelationalDatabaseConnectorConfig config(boolean schemaChangesEnabled) {
        final Configuration configuration = Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SIGNAL_DATA_COLLECTION, "debezium.signal")
                .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "core")
                .with(RelationalDatabaseConnectorConfig.INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES, schemaChangesEnabled)
                .build();
        return new RelationalDatabaseConnectorConfig(configuration, null, null, 0, ColumnFilterMode.CATALOG, true) {
            @Override
            public boolean supportsSchemaChangesDuringIncrementalSnapshot() {
                return true;
            }

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
}
