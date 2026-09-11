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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        jdbcConnection = mock(JdbcConnection.class);
        progressListener = mock(SnapshotProgressListener.class);
        notificationService = mock(NotificationService.class, RETURNS_DEEP_STUBS);
        source = new SignalBasedIncrementalSnapshotChangeEventSource<>(config(), jdbcConnection, null, null, null, progressListener, null,
                notificationService);

        // A snapshot with a single pending data collection so that readChunk proceeds past its
        // guard clauses and starts reading a chunk.
        SignalBasedIncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        context.addDataCollectionNamesToSnapshot("signal-1", List.of("public.a"), List.of(), "");

        offsetContext = mock(OffsetContext.class);
        doReturn(context).when(offsetContext).getIncrementalSnapshotContext();
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
    }

    @Test
    @FixFor("dbz#2275")
    public void shouldNotCloseConnectionWhenChunkReadFailsWithOtherSqlError() throws Exception {
        // A generic (potentially transient) SQL error is not a broken connection and must not cause
        // the connection to be discarded.
        when(jdbcConnection.commit()).thenThrow(new SQLException("transient failure"));

        source.readChunk(null, offsetContext);

        verify(jdbcConnection, never()).close();
    }

    @Test
    @FixFor("debezium/dbz#2604")
    @SuppressWarnings("unchecked")
    public void shouldEndChunkTransactionWhenChunkReadFailsWithNonSqlError() throws Exception {
        // A schema change racing with the chunk query surfaces as a runtime exception from result-set
        // processing ("Column 'c' not found in result set ...", see DBZ-4350). readChunk skips the
        // table and lets streaming continue, but the transaction the chunk queries run in must still
        // be ended: on connections with autocommit disabled it holds the chunk table's shared
        // metadata lock until this connection ends the transaction. Ending it here must not
        // depend on a later window or connector-specific streaming loop cleaning it up.
        RelationalDatabaseSchema schema = mock(RelationalDatabaseSchema.class);
        Table table = mock(Table.class);
        when(schema.tableFor(any(TableId.class))).thenReturn(table);

        ChunkQueryBuilder<TableId> chunkQueryBuilder = mock(ChunkQueryBuilder.class);
        doReturn(chunkQueryBuilder).when(jdbcConnection).chunkQueryBuilder(any());
        when(chunkQueryBuilder.prepareTable(any(), any())).thenReturn(table);
        when(chunkQueryBuilder.getQueryColumns(any(), any())).thenReturn(List.of(mock(Column.class)));
        when(chunkQueryBuilder.buildMaxPrimaryKeyQuery(any(), any(), any())).thenReturn("SELECT max(pk) FROM a");

        source = new SignalBasedIncrementalSnapshotChangeEventSource<>(config(), jdbcConnection, null, schema, null, progressListener, null,
                notificationService);

        final AtomicBoolean chunkReadFailed = new AtomicBoolean();
        final AtomicBoolean transactionEndedAfterFailure = new AtomicBoolean();
        when(jdbcConnection.queryAndMap(anyString(), any(JdbcConnection.ResultSetMapper.class))).thenAnswer(invocation -> {
            chunkReadFailed.set(true);
            throw new IllegalArgumentException("Column 'c' not found in result set 'pk, aa, c'");
        });
        when(jdbcConnection.rollback()).thenAnswer(invocation -> {
            if (chunkReadFailed.get()) {
                transactionEndedAfterFailure.set(true);
            }
            return jdbcConnection;
        });

        source.readChunk(null, offsetContext);

        assertThat(transactionEndedAfterFailure)
                .as("the transaction the failed chunk read ran in must be ended before readChunk returns")
                .isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2604")
    public void shouldCloseConnectionAndReportRollbackFailure() throws Exception {
        final var readFailure = new SQLException("chunk failed");
        final var rollbackFailure = new SQLException("rollback failed");
        when(jdbcConnection.commit()).thenThrow(readFailure);
        when(jdbcConnection.rollback()).thenThrow(rollbackFailure);
        final var queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(10).maxQueueSize(20).pollInterval(Duration.ofMillis(10))
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "recovery", "test"))
                .build();
        try {
            final var errorHandler = new ErrorHandler(SourceConnector.class, config(), queue, null);
            source.setErrorHandler(errorHandler);

            assertThatThrownBy(() -> source.readChunk(null, offsetContext))
                    .hasMessageContaining("Could not roll back")
                    .hasCause(rollbackFailure);

            verify(jdbcConnection).close();
            verify(progressListener, never()).snapshotCompleted(null);
            assertThat(rollbackFailure.getSuppressed()).containsExactly(readFailure);
            assertThat(errorHandler.getProducerThrowable()).hasCause(rollbackFailure);
            assertThatThrownBy(queue::poll).isInstanceOf(ConnectException.class);
        }
        finally {
            queue.close();
        }
    }

    private RelationalDatabaseConnectorConfig config() {
        final Configuration configuration = Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SIGNAL_DATA_COLLECTION, "debezium.signal")
                .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "core")
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
}
