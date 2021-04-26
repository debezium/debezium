/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotIsolationMode;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;

public class SqlServerSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<SqlServerTaskPartition, SqlServerOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSnapshotChangeEventSource.class);

    /**
     * Code 4096 corresponds to SNAPSHOT isolation level, which is not a part of the standard but SQL Server specific.
     */
    private static final int TRANSACTION_SNAPSHOT = 4096;

    private final SqlServerConnectorConfig connectorConfig;
    private final SqlServerConnection jdbcConnection;
    private final SqlServerDatabaseSchema sqlServerDatabaseSchema;
    private final Map<SqlServerTaskPartition, Map<TableId, SqlServerChangeTable>> changeTablesByPartition = new HashMap<>();

    public SqlServerSnapshotChangeEventSource(SqlServerConnectorConfig connectorConfig, SqlServerConnection jdbcConnection,
                                              SqlServerDatabaseSchema schema, EventDispatcher<SqlServerTaskPartition, SqlServerOffsetContext, TableId> dispatcher,
                                              Clock clock,
                                              SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, jdbcConnection, schema, dispatcher, clock, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.sqlServerDatabaseSchema = schema;
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(SqlServerOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.");
            snapshotSchema = false;
            snapshotData = false;
        }
        else {
            LOGGER.info("No previous offset has been found");
            if (connectorConfig.getSnapshotMode().includeData()) {
                LOGGER.info("According to the connector configuration both schema and data will be snapshotted");
            }
            else {
                LOGGER.info("According to the connector configuration only schema will be snapshotted");
            }
            snapshotData = connectorConfig.getSnapshotMode().includeData();
        }

        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext context, SqlServerTaskPartition partition) throws Exception {
        return new SqlServerSnapshotContext(partition.getDatabaseName());
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext snapshotContext) throws Exception {
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
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext ctx) throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, SqlServerTaskPartition partition, RelationalSnapshotContext snapshotContext)
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

                    String query = String.format("SELECT TOP(0) * FROM [%s].[%s].[%s] WITH (TABLOCKX)", partition.getDatabaseName(), tableId.schema(), tableId.table());
                    statement.executeQuery(query).close();
                }
            }
        }
        else {
            throw new IllegalStateException("Unknown locking mode specified.");
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext snapshotContext) throws SQLException {
        // Exclusive mode: locks should be kept until the end of transaction.
        // read_uncommitted mode; snapshot mode: no locks have been acquired.
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            jdbcConnection.connection().rollback(((SqlServerSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
            LOGGER.info("Schema locks released.");
        }
    }

    @Override
    protected void determineSnapshotOffset(SqlServerTaskPartition partition, SqlServerOffsetContext previousOffset, RelationalSnapshotContext ctx)
            throws Exception {
        ctx.offset = new SqlServerOffsetContext(
                connectorConfig,
                partition.getSourcePartition(),
                TxLogPosition.valueOf(jdbcConnection.getMaxLsn(partition.getDatabaseName())),
                false,
                false);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, SqlServerTaskPartition partition,
                                      SqlServerOffsetContext offsetContext, RelationalSnapshotContext snapshotContext)
            throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        Map<TableId, SqlServerChangeTable> changeTables = new HashMap<>();

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}'", snapshotContext.catalogName);
            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);

            // Save changeTables for sql select later.
            changeTables = jdbcConnection.listOfChangeTables(partition.getDatabaseName()).stream()
                    .collect(Collectors.toMap(SqlServerChangeTable::getSourceTableId, changeTable -> changeTable,
                            (changeTable1, changeTable2) -> changeTable1.getStartLsn().compareTo(changeTable2.getStartLsn()) > 0
                                    ? changeTable1
                                    : changeTable2));

            // Update table schemas to only include columns that are also included in the cdc tables.
            changeTables.forEach((tableId, sqlServerChangeTable) -> {
                Table sourceTable = snapshotContext.tables.forTable(tableId);
                // SourceTable will be null for tables that have cdc enabled, but are excluded in the configuration.
                if (sourceTable != null) {
                    List<Column> cdcEnabledSourceColumns = sourceTable.filterColumns(
                            column -> sqlServerChangeTable.getCapturedColumns().contains(column.name()));
                    snapshotContext.tables.overwriteTable(sourceTable.id(), cdcEnabledSourceColumns,
                            sourceTable.primaryKeyColumnNames(), sourceTable.defaultCharsetName());
                }
            });
        }

        changeTablesByPartition.put(partition, changeTables);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext snapshotContext, Table table) throws SQLException {
        return new SchemaChangeEvent(
                snapshotContext.offset.getPartition(),
                snapshotContext.offset.getOffset(),
                snapshotContext.offset.getSourceInfo(),
                snapshotContext.catalogName,
                table.id().schema(),
                null,
                table,
                SchemaChangeEventType.CREATE,
                true);
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {
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
     * Generate a valid sqlserver query string for the specified table
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(SqlServerTaskPartition partition, RelationalSnapshotContext snapshotContext, TableId tableId) {
        String modifiedColumns = checkExcludedColumns(partition, tableId);
        return Optional.of(String.format("SELECT %s FROM [%s].[%s].[%s]", modifiedColumns, partition.getDatabaseName(), tableId.schema(), tableId.table()));
    }

    @Override
    protected String enhanceOverriddenSelect(SqlServerTaskPartition partition, RelationalSnapshotContext snapshotContext,
                                             String overriddenSelect, TableId tableId) {
        String modifiedColumns = checkExcludedColumns(partition, tableId);
        return overriddenSelect.replaceAll("\\*", modifiedColumns);
    }

    private String checkExcludedColumns(SqlServerTaskPartition partition, TableId tableId) {
        Table table = sqlServerDatabaseSchema.tableFor(tableId);
        return table.retrieveColumnNames().stream()
                .filter(columnName -> filterChangeTableColumns(partition, tableId, columnName))
                .filter(columnName -> connectorConfig.getColumnFilter().matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName))
                .map(columnName -> {
                    StringBuilder sb = new StringBuilder();
                    if (!columnName.contains(tableId.table())) {
                        sb.append("[").append(tableId.table()).append("]")
                                .append(".[").append(columnName).append("]");
                    }
                    else {
                        sb.append("[").append(columnName).append("]");
                    }
                    return sb.toString();
                }).collect(Collectors.joining(","));
    }

    private boolean filterChangeTableColumns(SqlServerTaskPartition partition, TableId tableId, String columnName) {
        SqlServerChangeTable changeTable = changeTablesByPartition.get(partition).get(tableId);
        if (changeTable != null) {
            return changeTable.getCapturedColumns().contains(columnName);
        }
        // ChangeTable will be null if cdc has not been enabled for it yet.
        // Return true to allow columns to be captured.
        return true;
    }

    @Override
    protected Object getColumnValue(ResultSet rs, int columnIndex, Column column) throws SQLException {
        final ResultSetMetaData metaData = rs.getMetaData();
        final int columnType = metaData.getColumnType(columnIndex);

        if (columnType == Types.TIME) {
            return rs.getTimestamp(columnIndex);
        }
        else {
            return super.getColumnValue(rs, columnIndex, column);
        }
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class SqlServerSnapshotContext extends RelationalSnapshotContext {

        private int isolationLevelBeforeStart;
        private Savepoint preSchemaSnapshotSavepoint;

        public SqlServerSnapshotContext(String catalogName) throws SQLException {
            super(catalogName);
        }
    }

}
