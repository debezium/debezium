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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotIsolationMode;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;

public class SqlServerSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSnapshotChangeEventSource.class);

    /**
     * Code 4096 corresponds to SNAPSHOT isolation level, which is not a part of the standard but SQL Server specific.
     */
    private static final int TRANSACTION_SNAPSHOT = 4096;

    private final SqlServerConnectorConfig connectorConfig;
    private final SqlServerConnection jdbcConnection;

    public SqlServerSnapshotChangeEventSource(SqlServerConnectorConfig connectorConfig, SqlServerOffsetContext previousOffset, SqlServerConnection jdbcConnection,
                                              SqlServerDatabaseSchema schema, EventDispatcher<TableId> dispatcher, Clock clock,
                                              SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, previousOffset, jdbcConnection, schema, dispatcher, clock, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
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
    protected SnapshotContext prepare(ChangeEventSourceContext context) throws Exception {
        return new SqlServerSnapshotContext(jdbcConnection.getRealDatabaseName());
    }

    @Override
    protected void connectionCreated(SnapshotContext snapshotContext) throws Exception {
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
    protected Set<TableId> getAllTableIds(SnapshotContext ctx) throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_UNCOMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
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

                    String query = String.format("SELECT TOP(0) * FROM [%s].[%s] WITH (TABLOCKX)", tableId.schema(), tableId.table());
                    statement.executeQuery(query).close();
                }
            }
        }
        else {
            throw new IllegalStateException("Unknown locking mode specified.");
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(SnapshotContext snapshotContext) throws SQLException {
        // Exclusive mode: locks should be kept until the end of transaction.
        // read_uncommitted mode; snapshot mode: no locks have been acquired.
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            jdbcConnection.connection().rollback(((SqlServerSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
            LOGGER.info("Schema locks released.");
        }
    }

    @Override
    protected void determineSnapshotOffset(SnapshotContext ctx) throws Exception {
        ctx.offset = new SqlServerOffsetContext(
                connectorConfig,
                TxLogPosition.valueOf(jdbcConnection.getMaxLsn()),
                false,
                false);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

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
        }
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(SnapshotContext snapshotContext, Table table) throws SQLException {
        return new SchemaChangeEvent(snapshotContext.offset.getPartition(), snapshotContext.offset.getOffset(), snapshotContext.catalogName,
                table.id().schema(), null, table, SchemaChangeEventType.CREATE, true);
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
    protected Optional<String> getSnapshotSelect(SnapshotContext snapshotContext, TableId tableId) {
        return Optional.of(String.format("SELECT * FROM [%s].[%s]", tableId.schema(), tableId.table()));
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class SqlServerSnapshotContext extends SnapshotContext {

        private int isolationLevelBeforeStart;
        private Savepoint preSchemaSnapshotSavepoint;

        public SqlServerSnapshotContext(String catalogName) throws SQLException {
            super(catalogName);
        }
    }

}
