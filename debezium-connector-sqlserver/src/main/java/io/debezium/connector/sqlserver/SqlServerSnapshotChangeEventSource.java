/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotIsolationMode;
import io.debezium.connector.sqlserver.SqlServerOffsetContext.Loader;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

public class SqlServerSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<SqlServerPartition, SqlServerOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSnapshotChangeEventSource.class);

    /**
     * Code 4096 corresponds to SNAPSHOT isolation level, which is not a part of the standard but SQL Server specific.
     */
    private static final int TRANSACTION_SNAPSHOT = 4096;

    private final SqlServerConnectorConfig connectorConfig;
    private final SqlServerConnection jdbcConnection;
    private final SqlServerDatabaseSchema sqlServerDatabaseSchema;
    private final Map<SqlServerPartition, Map<TableId, SqlServerChangeTable>> changeTablesByPartition = new HashMap<>();

    public SqlServerSnapshotChangeEventSource(SqlServerConnectorConfig connectorConfig, MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory,
                                              SqlServerDatabaseSchema schema, EventDispatcher<SqlServerPartition, TableId> dispatcher, Clock clock,
                                              SnapshotProgressListener<SqlServerPartition> snapshotProgressListener,
                                              NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.sqlServerDatabaseSchema = schema;
    }

    @Override
    public SnapshottingTask getSnapshottingTask(SqlServerPartition partition, SqlServerOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();
        Map<String, String> snapshotSelectOverridesByTable = connectorConfig.getSnapshotSelectOverridesByTable().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().identifier(), Map.Entry::getValue));

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.");
            snapshotSchema = false;
            snapshotData = false;
        }
        else {
            LOGGER.info("No previous offset has been found");
            if (this.connectorConfig.getSnapshotMode().includeData()) {
                LOGGER.info("According to the connector configuration both schema and data will be snapshotted");
            }
            else {
                LOGGER.info("According to the connector configuration only schema will be snapshotted");
            }
            snapshotData = this.connectorConfig.getSnapshotMode().includeData();
        }

        return new SnapshottingTask(snapshotSchema, snapshotData, dataCollectionsToBeSnapshotted, snapshotSelectOverridesByTable, false);
    }

    @Override
    protected SnapshotContext<SqlServerPartition, SqlServerOffsetContext> prepare(SqlServerPartition partition)
            throws Exception {
        return new SqlServerSnapshotContext(partition);
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext)
            throws Exception {
        ((SqlServerSnapshotContext) snapshotContext).isolationLevelBeforeStart = jdbcConnection.connection().getTransactionIsolation();

        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.SNAPSHOT) {
            // Terminate any transaction in progress so we can change the isolation level
            jdbcConnection.connection().rollback();
            // With one exception, you can switch from one isolation level to another at any time during a transaction.
            // The exception occurs when changing from any isolation level to SNAPSHOT isolation.
            // That is why SNAPSHOT isolation level has to be set at the very beginning of the transaction.
            jdbcConnection.connection().setTransactionIsolation(TRANSACTION_SNAPSHOT);
        }
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_UNCOMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_COMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.SNAPSHOT) {
            // Snapshot transaction isolation level has already been set.
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.EXCLUSIVE
                || connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            LOGGER.info("Setting locking timeout to {} s", connectorConfig.snapshotLockTimeout().getSeconds());
            jdbcConnection.execute("SET LOCK_TIMEOUT " + connectorConfig.snapshotLockTimeout().toMillis());
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            ((SqlServerSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("dbz_schema_snapshot");

            LOGGER.info("Executing schema locking");
            try (Statement statement = jdbcConnection.connection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                for (TableId tableId : snapshotContext.capturedTables) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while locking table " + tableId);
                    }

                    LOGGER.info("Locking table {}", tableId);

                    String query = String.format("SELECT TOP(0) * FROM [%s].[%s].[%s] WITH (TABLOCKX)",
                            tableId.catalog(), tableId.schema(), tableId.table());
                    statement.executeQuery(query).close();
                }
            }
        }
        else {
            throw new IllegalStateException("Unknown locking mode specified.");
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext)
            throws SQLException {
        // Exclusive mode: locks should be kept until the end of transaction.
        // read_uncommitted mode; snapshot mode: no locks have been acquired.
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            jdbcConnection.connection().rollback(((SqlServerSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
            LOGGER.info("Schema locks released.");
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx,
                                           SqlServerOffsetContext previousOffset)
            throws Exception {
        ctx.offset = new SqlServerOffsetContext(
                connectorConfig,
                TxLogPosition.valueOf(jdbcConnection.getMaxLsn(ctx.partition.getDatabaseName())),
                false,
                false);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext,
                                      SqlServerOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {

        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // Save changeTables for sql select later.
        final Map<TableId, SqlServerChangeTable> changeTables = jdbcConnection
                .getChangeTables(snapshotContext.partition.getDatabaseName())
                .stream()
                .collect(Collectors.toMap(SqlServerChangeTable::getSourceTableId, changeTable -> changeTable,
                        (changeTable1, changeTable2) -> changeTable1.getStartLsn().compareTo(changeTable2.getStartLsn()) > 0
                                ? changeTable1
                                : changeTable2));

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}'", snapshotContext.catalogName);

            Tables.TableFilter tableFilter = snapshottingTask.isBlocking() ? Tables.TableFilter.fromPredicate(snapshotContext.capturedTables::contains)
                    : connectorConfig.getTableFilters().dataCollectionFilter();

            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    tableFilter,
                    null,
                    false);

            // Update table schemas to only include columns that are also included in the cdc tables.
            changeTables.forEach((tableId, sqlServerChangeTable) -> {
                Table sourceTable = snapshotContext.tables.forTable(tableId);
                // SourceTable will be null for tables that have cdc enabled, but are excluded in the configuration.
                if (sourceTable != null) {
                    final List<Column> cdcEnabledSourceColumns = sourceTable.filterColumns(
                            column -> sqlServerChangeTable.getCapturedColumns().contains(column.name()));
                    final List<String> cdcEnabledPkColumns = sourceTable.primaryKeyColumnNames().stream()
                            .filter(column -> sqlServerChangeTable.getCapturedColumns().contains(column))
                            .collect(Collectors.toList());

                    snapshotContext.tables.overwriteTable(sourceTable.id(), cdcEnabledSourceColumns,
                            cdcEnabledPkColumns, sourceTable.defaultCharsetName(), sourceTable.attributes());
                }
            });
        }

        changeTablesByPartition.put(snapshotContext.partition, changeTables);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext,
                                                    Table table)
            throws SQLException {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected void completed(SnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext) {
        close(snapshotContext);
    }

    @Override
    protected void aborted(SnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext) {
        close(snapshotContext);
    }

    private void close(SnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext) {
        try {
            jdbcConnection.connection().setTransactionIsolation(((SqlServerSnapshotContext) snapshotContext).isolationLevelBeforeStart);
            LOGGER.info("Removing locking timeout");
            jdbcConnection.execute("SET LOCK_TIMEOUT -1");
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to set transaction isolation level.", e);
        }
    }

    /**
     * Generate a valid SQL Server query string for the specified table
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        String snapshotSelectColumns = columns.stream()
                .collect(Collectors.joining(", "));
        return Optional.of(String.format("SELECT %s FROM [%s].[%s].[%s]", snapshotSelectColumns, tableId.catalog(), tableId.schema(), tableId.table()));
    }

    @Override
    protected String enhanceOverriddenSelect(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext,
                                             String overriddenSelect, TableId tableId) {
        String snapshotSelectColumns = getPreparedColumnNames(snapshotContext.partition, sqlServerDatabaseSchema.tableFor(tableId)).stream()
                .collect(Collectors.joining(", "));
        return overriddenSelect.replaceAll(SELECT_ALL_PATTERN.pattern(), snapshotSelectColumns);
    }

    @Override
    protected boolean additionalColumnFilter(SqlServerPartition partition, TableId tableId, String columnName) {
        return filterChangeTableColumns(partition, tableId, columnName);
    }

    private boolean filterChangeTableColumns(SqlServerPartition partition, TableId tableId, String columnName) {
        SqlServerChangeTable changeTable = changeTablesByPartition.get(partition).get(tableId);
        if (changeTable != null) {
            return changeTable.getCapturedColumns().contains(columnName);
        }
        // ChangeTable will be null if cdc has not been enabled for it yet.
        // Return true to allow columns to be captured.
        return true;
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class SqlServerSnapshotContext extends RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> {

        private int isolationLevelBeforeStart;
        private Savepoint preSchemaSnapshotSavepoint;

        SqlServerSnapshotContext(SqlServerPartition partition) throws SQLException {
            super(partition, partition.getDatabaseName());
        }
    }

    @Override
    protected SqlServerOffsetContext copyOffset(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}
