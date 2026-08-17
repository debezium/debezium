/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactory;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.chunked.SnapshotChunk;
import io.debezium.pipeline.source.snapshot.chunked.SnapshotProgress;
import io.debezium.pipeline.source.snapshot.chunked.TableChunkProgress;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

/**
 * A {@link StreamingChangeEventSource} for Oracle.
 *
 * @author Gunnar Morling
 */
public class OracleSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSnapshotChangeEventSource.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnectionFactory connectionFactory;
    private final OracleConnection jdbcConnection;
    private final OracleDatabaseSchema databaseSchema;
    private final Map<String, OracleConnection> pdbLockConnections = new LinkedHashMap<>();

    public OracleSnapshotChangeEventSource(OracleConnectorConfig connectorConfig, OracleConnectionFactory connectionFactory,
                                           OracleDatabaseSchema schema, EventDispatcher<OraclePartition, TableId> dispatcher, Clock clock,
                                           SnapshotProgressListener<OraclePartition> snapshotProgressListener,
                                           NotificationService<OraclePartition, OracleOffsetContext> notificationService, SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.connectionFactory = connectionFactory;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.databaseSchema = schema;
    }

    @Override
    protected SnapshotContext<OraclePartition, OracleOffsetContext> prepare(OraclePartition partition, boolean onDemand) {
        final List<String> pdbNames = connectorConfig.getPdbNames();
        if (pdbNames.size() == 1) {
            // All snapshot operations occur within the single container, pin for the snapshot's duration
            jdbcConnection.setSessionToPdb(pdbNames.get(0));
        }
        else if (pdbNames.size() > 1) {
            // Operations that require a specific container pin the session on demand and restore it;
            // anything else, such as resolving the snapshot offset, must observe the entire container
            // hierarchy, so the main connection is anchored to the root container.
            jdbcConnection.resetSessionToCdb();
        }

        // With multiple pluggable databases there is no single catalog; the empty sentinel mirrors the
        // binlog connector's multi-catalog convention and the catalog of record is each TableId's.
        final String catalogName = pdbNames.size() > 1 ? "" : connectorConfig.getDefaultCatalogName();
        return new OracleSnapshotContext(partition, catalogName, onDemand);
    }

    @Override
    protected void connectionPoolConnectionCreated(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                                   JdbcConnection connection) {
        // With multiple pluggable databases the pool connections cannot be pinned up front; they are
        // pinned per table or per chunk as each unit of work is executed.
        final List<String> pdbNames = connectorConfig.getPdbNames();
        if (pdbNames.size() == 1) {
            ((OracleConnection) connection).setSessionToPdb(pdbNames.get(0));
        }
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx)
            throws Exception {
        // getAllTableIds is preferred over readTableNames as the latter is very slow, taking upwards
        // of 30 minutes on an instance with 600 tables
        final boolean multiplePdbNames = connectorConfig.getPdbNames().size() > 1;
        try {
            final Set<TableId> tableIds = new HashSet<>();
            for (String catalogName : connectorConfig.getCatalogNames()) {
                if (multiplePdbNames) {
                    jdbcConnection.setSessionToPdb(catalogName);
                }
                tableIds.addAll(jdbcConnection.getAllTableIds(catalogName));
            }
            return tableIds;
        }
        finally {
            if (multiplePdbNames) {
                jdbcConnection.resetSessionToCdb();
            }
        }
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        if (!connectorConfig.getSnapshotLockingMode().get().usesLocking()) {
            LOGGER.info("Schema locking was disabled in connector configuration");
            return;
        }

        if (connectorConfig.getPdbNames().size() > 1) {
            // A transaction cannot span containers, so each pluggable database's tables are locked on a
            // dedicated connection whose transaction is held until the schema snapshot completes.
            final Map<String, List<TableId>> tablesByCatalog = snapshotContext.capturedTables.stream()
                    .collect(Collectors.groupingBy(TableId::catalog, LinkedHashMap::new, Collectors.toList()));
            for (Map.Entry<String, List<TableId>> catalogTables : tablesByCatalog.entrySet()) {
                final OracleConnection connection = connectionFactory.newConnection();
                pdbLockConnections.put(catalogTables.getKey(), connection);
                connection.setSessionToPdb(catalogTables.getKey());
                lockTables(sourceContext, connection, catalogTables.getValue());
            }
        }
        else {
            ((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("dbz_schema_snapshot");
            lockTables(sourceContext, jdbcConnection, snapshotContext.capturedTables);
        }
    }

    /**
     * Acquires the snapshot lock for each of the specified tables using the given connection.
     *
     * @param sourceContext the change event source context
     * @param connection the connection whose transaction should hold the locks
     * @param tableIds the tables to be locked
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the thread is interrupted
     */
    private void lockTables(ChangeEventSourceContext sourceContext, OracleConnection connection, Collection<TableId> tableIds)
            throws SQLException, InterruptedException {
        try (Statement statement = connection.connection().createStatement()) {
            for (TableId tableId : tableIds) {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while locking table " + tableId);
                }

                Optional<String> lockingStatement = snapshotterService.getSnapshotLock().tableLockingStatement(null, quote(tableId));
                if (lockingStatement.isPresent()) {
                    LOGGER.debug("Locking table {}", tableId);
                    statement.execute(lockingStatement.get());
                }
            }
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext)
            throws SQLException {
        if (!connectorConfig.getSnapshotLockingMode().get().usesLocking()) {
            return;
        }

        if (connectorConfig.getPdbNames().size() > 1) {
            try {
                for (Map.Entry<String, OracleConnection> lockConnection : pdbLockConnections.entrySet()) {
                    if (!lockConnection.getValue().isConnected()) {
                        // The server rolls back a lost connection's transaction, silently releasing its locks;
                        // fail so the snapshot is retried rather than risk an inconsistent schema.
                        throw new DebeziumException("Lock connection for pluggable database '" + lockConnection.getKey()
                                + "' was lost during the schema snapshot");
                    }
                    lockConnection.getValue().connection().rollback();
                }
            }
            finally {
                closePdbLockConnections();
            }
        }
        else {
            jdbcConnection.connection().rollback(((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
        }
    }

    private void closePdbLockConnections() {
        for (Map.Entry<String, OracleConnection> lockConnection : pdbLockConnections.entrySet()) {
            try {
                lockConnection.getValue().close();
            }
            catch (SQLException e) {
                LOGGER.warn("Failed to close lock connection for pluggable database '{}'", lockConnection.getKey(), e);
            }
        }
        pdbLockConnections.clear();
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx,
                                           OracleOffsetContext previousOffset)
            throws Exception {

        if (previousOffset != null && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot()) {
            ctx.offset = previousOffset;
            tryStartingSnapshot(ctx);
            return;
        }

        ctx.offset = connectorConfig.getAdapter().determineSnapshotOffset(ctx, connectorConfig, jdbcConnection);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                      OracleOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {
        Set<TableId> capturedSchemaTables;
        if (databaseSchema.storeOnlyCapturedTables()) {
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Only captured tables schema should be captured, capturing: {}", capturedSchemaTables);
        }
        else {
            capturedSchemaTables = snapshotContext.capturedSchemaTables;
            LOGGER.info("All eligible tables schema should be captured, capturing: {}", capturedSchemaTables);
        }

        final Tables.TableFilter tableFilter = getTableFilter(snapshottingTask, snapshotContext);
        if (connectorConfig.getPdbNames().size() > 1) {
            final Map<String, Set<String>> schemasByCatalog = capturedSchemaTables.stream()
                    .collect(Collectors.groupingBy(TableId::catalog, Collectors.mapping(TableId::schema, Collectors.toSet())));
            for (Map.Entry<String, Set<String>> catalogSchemas : schemasByCatalog.entrySet()) {
                jdbcConnection.setSessionToPdb(catalogSchemas.getKey());
                readSchemas(sourceContext, snapshotContext, catalogSchemas.getValue(), tableFilter);
            }
        }
        else {
            Set<String> schemas = capturedSchemaTables.stream().map(TableId::schema).collect(Collectors.toSet());
            readSchemas(sourceContext, snapshotContext, schemas, tableFilter);
        }
    }

    private void readSchemas(ChangeEventSourceContext sourceContext,
                             RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                             Set<String> schemas, Tables.TableFilter tableFilter)
            throws SQLException, InterruptedException {
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }
            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    null,
                    schema,
                    tableFilter,
                    null,
                    false);
        }
    }

    private Tables.TableFilter getTableFilter(SnapshottingTask snapshottingTask, RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext) {

        if (snapshottingTask.isOnDemand()) {
            return Tables.TableFilter.fromPredicate(snapshotContext.capturedTables::contains);
        }

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        // however, for users interested only in captured tables, we need to pass also table filter
        return connectorConfig.storeOnlyCapturedTables() ? connectorConfig.getTableFilters().dataCollectionFilter() : null;
    }

    @Override
    protected String enhanceOverriddenSelect(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                             String overriddenSelect, TableId tableId) {
        String snapshotOffset = (String) snapshotContext.offset.getOffset().get(SourceInfo.SCN_KEY);
        String token = connectorConfig.getTokenToReplaceInSnapshotPredicate();
        if (token != null) {
            return overriddenSelect.replaceAll(token, " AS OF SCN " + snapshotOffset);
        }
        return overriddenSelect;
    }

    @Override
    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext) {
        return snapshotContext.capturedSchemaTables;
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                                    Table table)
            throws SQLException {
        setSessionToTablePdb(jdbcConnection, table.id());
        return SchemaChangeEvent.ofCreate(
                snapshotContext.partition,
                snapshotContext.offset,
                table.id().catalog(),
                table.id().schema(),
                jdbcConnection.getTableMetadataDdl(table.id()),
                table,
                true);
    }

    /**
     * Pins the connection's session to the pluggable database that contains the specified table when
     * multiple pluggable databases are configured; no-op otherwise as the session is already pinned.
     *
     * @param connection the connection whose session should be pinned
     * @param tableId the table whose pluggable database should be made current
     */
    private void setSessionToTablePdb(OracleConnection connection, TableId tableId) {
        if (connectorConfig.getPdbNames().size() > 1 && !tableId.catalog().equals(connection.getSessionPdbName())) {
            connection.setSessionToPdb(tableId.catalog());
        }
    }

    @Override
    protected Instant getSnapshotSourceTimestamp(JdbcConnection jdbcConnection, OracleOffsetContext offset, TableId tableId) {
        try {
            final OracleConnection oracleConnection = (OracleConnection) jdbcConnection;
            return oracleConnection.getScnToTimestamp(offset.getScn())
                    .orElseThrow(() -> new ConnectException("Failed reading SCN timestamp from database"))
                    // Database host timezone adjustment
                    .minusSeconds(oracleConnection.getDatabaseSystemTime().getOffset().getTotalSeconds())
                    // JVM timezone adjustment
                    .plusSeconds(ZoneId.systemDefault().getRules().getOffset(Instant.now()).getTotalSeconds());
        }
        catch (SQLException e) {
            throw new ConnectException("Failed reading SCN timestamp from source database", e);
        }
    }

    /**
     * Generate a valid Oracle query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {

        return snapshotterService.getSnapshotQuery().snapshotQuery(quote(tableId), columns);
    }

    @Override
    protected Long rowCountForTableChunked(TableId tableId) throws SQLException {
        setSessionToTablePdb(jdbcConnection, tableId);
        // Oracle TableIds carry a CDB/PDB catalog that cannot appear in a qualified name; strip it
        // before quoting (as getSnapshotSelect does), otherwise the shared implementation would emit
        // an invalid "catalog"."schema"."table".
        return jdbcConnection.getRowCount(new TableId(null, tableId.schema(), tableId.table()));
    }

    @Override
    protected List<Column> getKeyColumnsForChunking(Table table) {
        // This hook is invoked at the start of each table's chunk boundary preparation, and the boundary
        // queries that follow run on the main connection; there is no dedicated per-table hook in the
        // chunked flow, so the session is pinned to the table's container here.
        setSessionToTablePdb(jdbcConnection, table.id());
        return super.getKeyColumnsForChunking(table);
    }

    @Override
    protected void doCreateDataEventsForChunk(ChangeEventSourceContext sourceContext,
                                              RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                              OracleOffsetContext offset, EventDispatcher.SnapshotReceiver<OraclePartition> snapshotReceiver,
                                              SnapshotChunk chunk, Map<TableId, TableChunkProgress> progressMap,
                                              SnapshotProgress snapshotProgress, JdbcConnection jdbcConnection)
            throws InterruptedException, SQLException {
        setSessionToTablePdb((OracleConnection) jdbcConnection, chunk.getTableId());
        super.doCreateDataEventsForChunk(sourceContext, snapshotContext, offset, snapshotReceiver, chunk, progressMap, snapshotProgress, jdbcConnection);
    }

    @Override
    protected List<Pattern> getSignalDataCollectionPattern(String signalingDataCollection) {
        // Oracle expects this value to be supplied using "<database>.<schema>.<table>"; however the
        // TableIdMapper used by the connector uses only "<schema>.<table>". This primarily targets
        // a fix for this specific use case as a much larger refactor is likely necessary long term.
        final TableId tableId = TableId.parse(signalingDataCollection);
        return Strings.listOfRegex(tableId.schema() + "." + tableId.table(), Pattern.CASE_INSENSITIVE);
    }

    @Override
    public void close() {
        closePdbLockConnections();
        if (connectorConfig.isUsingPluggableDatabase()) {
            jdbcConnection.resetSessionToCdb();
        }
    }

    private String quote(TableId tableId) {
        return new TableId(null, tableId.schema(), tableId.table()).toDoubleQuotedString();
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class OracleSnapshotContext extends RelationalSnapshotContext<OraclePartition, OracleOffsetContext> {

        private Savepoint preSchemaSnapshotSavepoint;

        OracleSnapshotContext(OraclePartition partition, String catalogName, boolean onDemand) {
            super(partition, catalogName, onDemand);
        }
    }

    @Override
    protected OracleOffsetContext copyOffset(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext) {
        return connectorConfig.getAdapter().copyOffset(connectorConfig, snapshotContext.offset);
    }

    @Override
    protected Callable<Void> createDataEventsForTableCallable(ChangeEventSourceContext sourceContext,
                                                              RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                                              EventDispatcher.SnapshotReceiver<OraclePartition> snapshotReceiver, Table table,
                                                              boolean firstTable, boolean lastTable, int tableOrder, int tableCount,
                                                              String selectStatement, OptionalLong rowCount, Set<TableId> rowCountKeySet,
                                                              Queue<JdbcConnection> connectionPool, Queue<OracleOffsetContext> offsets) {
        return () -> {
            JdbcConnection connection = connectionPool.poll();
            OracleOffsetContext offset = offsets.poll();
            try {
                setSessionToTablePdb((OracleConnection) connection, table.id());

                final int maxRetries = getTableSnapshotMaxRetries();
                final Metronome retrySleeper = Metronome.sleeper(Duration.ofSeconds(5), clock);

                for (int i = 0; i <= maxRetries; i++) {
                    try {
                        doCreateDataEventsForTable(sourceContext, snapshotContext, offset, snapshotReceiver, table, firstTable,
                                lastTable, tableOrder, tableCount, selectStatement, rowCount, rowCountKeySet, connection);
                        break;
                    }
                    catch (SQLException e) {
                        notificationService.initialSnapshotNotificationService().notifyCompletedTableWithError(snapshotContext.partition,
                                snapshotContext.offset,
                                table.id().identifier());

                        if (maxRetries > 0 && isTableSnapshotErrorRetriable(e)) {
                            if ((i + 1) <= maxRetries) {
                                LOGGER.warn("Table {} snapshot failed: {}, attempting to retry ({} of {})",
                                        table.id(), e.getMessage(), i, getTableSnapshotMaxRetries());
                                retrySleeper.pause();
                                continue;
                            }
                        }

                        throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
                    }
                }
            }
            finally {
                offsets.add(offset);
                connectionPool.add(connection);
            }
            return null;
        };
    }

    /**
     * Return the number of times the table's snapshot should be retried.
     *
     * @return the maximum number of snapshot retry attempts.
     */
    private int getTableSnapshotMaxRetries() {
        return connectorConfig.getSnapshotRetryDatabaseErrorsMaxRetries();
    }

    /**
     * Returns whether the specified table snapshot exception is retriable.
     *
     * @param exception the exception that was thrown
     * @return true if the exception should trigger a retry, false if the exception should fail
     */
    protected boolean isTableSnapshotErrorRetriable(SQLException exception) {
        // ORA-01466 - the table's metadata changed during the flashback query.
        // Attempt to recover by having the caller restart the table's snapshot from the beginning.
        return exception.getErrorCode() == 1466;
    }
}
