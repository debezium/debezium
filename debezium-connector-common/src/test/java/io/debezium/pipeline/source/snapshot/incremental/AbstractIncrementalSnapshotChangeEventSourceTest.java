/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.ExtendedMetadata;
import io.debezium.relational.ExtendedMetadata.ExtendedColumn;
import io.debezium.relational.ExtendedMetadata.ExtendedTable;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalMetadataFactory;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

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
    @FixFor("debezium/dbz#2843")
    @MockitoSettings(strictness = Strictness.LENIENT) // the offset context stubbed in setUp is not needed here
    public void shouldReadTableSchemaThroughConfiguredMetadataFactory() throws Exception {
        final TableId tableId = new TableId(null, "public", "a");
        final RelationalDatabaseConnectorConfig config = new TestRelationalDatabaseConfig(config().getConfig(), null, null, 0) {
            @Override
            public RelationalMetadataFactory getRelationalMetadataFactory() {
                return ExtendedMetadata.ExtendedRelationalMetadataFactory.INSTANCE;
            }
        };
        // Stands in for the JDBC metadata read: the table is created through the Tables instance the
        // source hands in, which is what decides the table type.
        final JdbcConnection connection = new JdbcConnection(config.getJdbcConfig(), c -> null, "\"", "\"") {
            @Override
            public void readSchema(Tables tables, String catalogName, String schemaName, Tables.TableFilter tableFilter,
                                   Tables.ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc) {
                tables.overwriteTable(tableId, List.of(ExtendedMetadata.column("id", "eu")), List.of("id"), null, List.of());
            }
        };
        final SignalBasedIncrementalSnapshotChangeEventSource<TestPartition, TableId> factorySource = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                config, connection, null, null, null, progressListener, null, notificationService);

        final Table table = factorySource.readSchemaForTable(tableId);

        assertThat(table).isInstanceOf(ExtendedTable.class);
        assertThat(table.columnWithName("id")).isInstanceOf(ExtendedColumn.class);
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
