/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

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

import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotIsolationMode;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;

public class Db2SnapshotChangeEventSource extends RelationalSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(Db2SnapshotChangeEventSource.class);

    private final Db2ConnectorConfig connectorConfig;
    private final Db2Connection jdbcConnection;

    public Db2SnapshotChangeEventSource(Db2ConnectorConfig connectorConfig, Db2OffsetContext previousOffset, Db2Connection jdbcConnection, Db2DatabaseSchema schema,
                                        EventDispatcher<TableId> dispatcher, Clock clock, SnapshotProgressListener snapshotProgressListener) {
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
        return new Db2SnapshotContext(jdbcConnection.getRealDatabaseName());
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext snapshotContext) throws Exception {
        ((Db2SnapshotContext) snapshotContext).isolationLevelBeforeStart = jdbcConnection.connection().getTransactionIsolation();
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext ctx) throws Exception {
        return jdbcConnection.readTableNames(null, null, null, new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext)
            throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_UNCOMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_COMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.EXCLUSIVE
                || connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            ((Db2SnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("db2_schema_snapshot");

            LOGGER.info("Executing schema locking");
            try (Statement statement = jdbcConnection.connection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                for (TableId tableId : snapshotContext.capturedTables) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while locking table " + tableId);
                    }

                    LOGGER.info("Locking table {}", tableId);

                    String query = String.format("SELECT * FROM %s.%s WHERE 0=1 WITH CS", tableId.schema(), tableId.table());
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
        // read_uncommitted mode; read_committed mode: no locks have been acquired.
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            jdbcConnection.connection().rollback(((Db2SnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
            LOGGER.info("Schema locks released.");
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext ctx) throws Exception {
        ctx.offset = new Db2OffsetContext(
                connectorConfig,
                TxLogPosition.valueOf(jdbcConnection.getMaxLsn()),
                false,
                false);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext) throws SQLException, InterruptedException {
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

            LOGGER.info("Reading structure of schema '{}'", schema);

            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    null,
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
        }
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
                SchemaChangeEventType.CREATE, true);
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {
        try {
            jdbcConnection.connection().setTransactionIsolation(((Db2SnapshotContext) snapshotContext).isolationLevelBeforeStart);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to set transaction isolation level.", e);
        }
    }

    /**
     * Generate a valid db2 query string for the specified table
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext snapshotContext, TableId tableId) {
        return Optional.of(String.format("SELECT * FROM %s.%s", tableId.schema(), tableId.table()));
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class Db2SnapshotContext extends RelationalSnapshotContext {

        private int isolationLevelBeforeStart;
        private Savepoint preSchemaSnapshotSavepoint;

        public Db2SnapshotContext(String catalogName) throws SQLException {
            super(catalogName);
        }
    }

}
