/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * Tests for {@link AbstractIncrementalSnapshotChangeEventSource} that drive the source through its
 * public entry points with a mocked environment, covering behaviour that cannot be reproduced in a
 * connector integration test.
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
        offsetContext = mock(OffsetContext.class);
        source = newSource(null);
    }

    private SignalBasedIncrementalSnapshotChangeEventSource<TestPartition, TableId> newSource(RelationalDatabaseSchema databaseSchema) {
        return new SignalBasedIncrementalSnapshotChangeEventSource<>(config(), jdbcConnection, null, databaseSchema, null, progressListener, null,
                notificationService);
    }

    /**
     * Puts a snapshot of the given data collections in progress on {@link #offsetContext}.
     */
    private SignalBasedIncrementalSnapshotContext<TableId> snapshotInProgressOf(String... dataCollectionIds) {
        SignalBasedIncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        context.addDataCollectionNamesToSnapshot("signal-1", List.of(dataCollectionIds), List.of(), "");
        doReturn(context).when(offsetContext).getIncrementalSnapshotContext();
        return context;
    }

    @Test
    @FixFor("dbz#2275")
    public void shouldCloseConnectionWhenChunkReadFailsWithNonTransientConnectionError() throws Exception {
        // A snapshot with a single pending data collection so that readChunk proceeds past its
        // guard clauses and starts reading a chunk.
        snapshotInProgressOf("public.a");

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
        snapshotInProgressOf("public.a");

        // A generic (potentially transient) SQL error is not a broken connection and must not cause
        // the connection to be discarded.
        when(jdbcConnection.commit()).thenThrow(new SQLException("transient failure"));

        source.readChunk(null, offsetContext);

        verify(jdbcConnection, never()).close();
    }

    /**
     * Both the start- and the stop-snapshot signal identify their data collections with regular expressions,
     * which are expanded to the ids known to the database schema. On a connector whose table ids are
     * case-insensitive (MySQL with {@code lower_case_table_names} set to a non-zero value) the schema keys
     * its tables by the lower-cased ids, while the tables themselves retain the case they were declared
     * with. Unless both signals expand to the very same id, the stop signal does not find the collection it
     * is to remove, the aborted snapshot keeps its collections and leaks into the next one.
     */
    @ParameterizedTest(name = "started with \"{0}\"")
    @ValueSource(strings = { ".*", "testdb\\..*", "testdb\\.mytable", "testdb.MyTable", "testdb.MYTABLE" })
    @FixFor("dbz#1563")
    public void shouldStopSnapshotOfMixedCaseTableWhenTableIdsAreCaseInsensitive(String startedWith) throws Exception {
        final SignalBasedIncrementalSnapshotContext<TableId> context = pausedSnapshotContext();
        source = newSource(caseInsensitiveSchemaContaining(new TableId("testdb", null, "MyTable")));

        startSnapshotOf(startedWith);
        assertThat(context.snapshotRunning()).isTrue();

        source.requestStopSnapshot(null, offsetContext, Map.of(), List.of(".*"));
        source.readChunk(null, offsetContext);

        assertThat(context.snapshotRunning()).isFalse();
    }

    /**
     * A regular expression that matches none of the data collections known to the database schema is kept
     * as it is, even though it does not parse as a table id. It is up to the snapshot context to skip it,
     * without affecting the other data collections of the signal.
     */
    @Test
    @FixFor("dbz#1563")
    public void shouldSkipRegexMatchingNoTable() throws Exception {
        final SignalBasedIncrementalSnapshotContext<TableId> context = pausedSnapshotContext();
        source = newSource(caseInsensitiveSchemaContaining(new TableId("testdb", null, "MyTable")));

        startSnapshotOf("otherdb\\..*", "testdb.MYTABLE");

        assertThat(context.getDataCollections())
                .extracting(DataCollection::getId)
                .containsExactly(new TableId("testdb", null, "MyTable"));
    }

    /**
     * Puts an empty, paused snapshot context on {@link #offsetContext}. Being paused keeps the source from
     * doing anything beyond processing the signals it is sent, so that the outcome of a test is the one of
     * those signals alone.
     */
    private SignalBasedIncrementalSnapshotContext<TableId> pausedSnapshotContext() {
        final SignalBasedIncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        context.pauseSnapshot();
        doReturn(context).when(offsetContext).getIncrementalSnapshotContext();
        return context;
    }

    private void startSnapshotOf(String... dataCollectionIds) throws InterruptedException {
        source.addDataCollectionNamesToSnapshot(
                new SignalPayload<>(null, "signal-1", "execute-snapshot", null, offsetContext, Map.of()),
                SnapshotConfiguration.Builder.builder().dataCollections(List.of(dataCollectionIds)).surrogateKey("").build());
    }

    /**
     * A schema that holds the given tables the way a connector with case-insensitive table ids does: the
     * ids are lower-cased, the tables keep their original case.
     */
    private RelationalDatabaseSchema caseInsensitiveSchemaContaining(TableId... tableIds) {
        final Tables tables = new Tables(true);
        for (TableId tableId : tableIds) {
            tables.overwriteTable(Table.editor().tableId(tableId).create());
        }

        final RelationalDatabaseSchema databaseSchema = mock(RelationalDatabaseSchema.class);
        when(databaseSchema.tableIds()).thenReturn(tables.tableIds());
        when(databaseSchema.tableFor(any())).thenAnswer(invocation -> tables.forTable(invocation.getArgument(0, TableId.class)));
        return databaseSchema;
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
