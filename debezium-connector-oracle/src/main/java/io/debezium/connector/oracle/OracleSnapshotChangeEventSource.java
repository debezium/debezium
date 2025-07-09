/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
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

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * A {@link StreamingChangeEventSource} for Oracle.
 *
 * @author Gunnar Morling
 */
public class OracleSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSnapshotChangeEventSource.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;
    private final OracleDatabaseSchema databaseSchema;

    public OracleSnapshotChangeEventSource(OracleConnectorConfig connectorConfig, DualOracleConnectionFactory<OracleConnection> connectionFactory,
                                           OracleDatabaseSchema schema, EventDispatcher<OraclePartition, TableId> dispatcher, Clock clock,
                                           SnapshotProgressListener<OraclePartition> snapshotProgressListener,
                                           NotificationService<OraclePartition, OracleOffsetContext> notificationService, SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.databaseSchema = schema;
    }

    @Override
    protected SnapshotContext<OraclePartition, OracleOffsetContext> prepare(OraclePartition partition, boolean onDemand) {
        if (!Strings.isNullOrBlank(connectorConfig.getPdbName())) {
            jdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
        }

        return new OracleSnapshotContext(partition, connectorConfig.getCatalogName(), onDemand);
    }

    @Override
    protected void connectionPoolConnectionCreated(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                                   JdbcConnection connection) {
        if (!Strings.isNullOrBlank(connectorConfig.getPdbName())) {
            ((OracleConnection) connection).setSessionToPdb(connectorConfig.getPdbName());
        }
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.getAllTableIds(ctx.catalogName);
        // this very slow approach(commented out), it took 30 minutes on an instance with 600 tables
        // return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"} );
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotLockingMode().get().usesLocking()) {
            ((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("dbz_schema_snapshot");

            try (Statement statement = jdbcConnection.connection().createStatement()) {
                for (TableId tableId : snapshotContext.capturedTables) {
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
        else {
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext)
            throws SQLException {
        if (connectorConfig.getSnapshotLockingMode().get().usesLocking()) {
            jdbcConnection.connection().rollback(((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
        }
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

        Set<String> schemas = capturedSchemaTables.stream().map(TableId::schema).collect(Collectors.toSet());

        final Tables.TableFilter tableFilter = getTableFilter(snapshottingTask, snapshotContext);
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
        return SchemaChangeEvent.ofCreate(
                snapshotContext.partition,
                snapshotContext.offset,
                snapshotContext.catalogName,
                table.id().schema(),
                jdbcConnection.getTableMetadataDdl(table.id()),
                table,
                true);
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
    protected List<Pattern> getSignalDataCollectionPattern(String signalingDataCollection) {
        // Oracle expects this value to be supplied using "<database>.<schema>.<table>"; however the
        // TableIdMapper used by the connector uses only "<schema>.<table>". This primarily targets
        // a fix for this specific use case as a much larger refactor is likely necessary long term.
        final TableId tableId = TableId.parse(signalingDataCollection);
        return Strings.listOfRegex(tableId.schema() + "." + tableId.table(), Pattern.CASE_INSENSITIVE);
    }

    @Override
    public void close() {
        if (!Strings.isNullOrBlank(connectorConfig.getPdbName())) {
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
                final int maxRetries = getTableSnapshotMaxRetries();
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
