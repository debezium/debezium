/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.SchemaChangeEvent;

/**
 * Base class for {@link SnapshotChangeEventSource} for relational databases with a schema history.
 *
 * @author Gunnar Morling
 */
// TODO Mostly, this should be usable for Postgres as well; only the aspect of managing the schema history will have to
// be made optional based on the connector
public abstract class HistorizedRelationalSnapshotChangeEventSource implements SnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(HistorizedRelationalSnapshotChangeEventSource.class);

    private final RelationalDatabaseConnectorConfig connectorConfig;
    private final OffsetContext previousOffset;
    private final JdbcConnection jdbcConnection;
    private final HistorizedRelationalDatabaseSchema schema;

    public HistorizedRelationalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig connectorConfig, OffsetContext previousOffset, JdbcConnection jdbcConnection, HistorizedRelationalDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.previousOffset = previousOffset;
        this.jdbcConnection = jdbcConnection;
        this.schema = schema;
    }

    @Override
    public SnapshotResult execute(ChangeEventSourceContext context) throws InterruptedException {
        // for now, just simple schema snapshotting is supported which just needs to be done once
        if (previousOffset != null) {
            LOGGER.debug("Found previous offset, skipping snapshotting");
            return SnapshotResult.completed(previousOffset);
        }

        Connection connection = null;

        try (SnapshotContext ctx = prepare(context)) {
            LOGGER.info("Snapshot step 1 - Preparing");

            connection = jdbcConnection.connection();
            connection.setAutoCommit(false);

            LOGGER.info("Snapshot step 2 - Determining captured tables");

            // Note that there's a minor race condition here: a new table matching the filters could be created between
            // this call and the determination of the initial snapshot position below; this seems acceptable, though
            determineCapturedTables(ctx);

            LOGGER.info("Snapshot step 3 - Locking captured tables");

            if (!lockTables(context, ctx)) {
                return SnapshotResult.aborted();
            }

            LOGGER.info("Snapshot step 4 - Determining snapshot offset");

            determineSnapshotOffset(ctx);

            LOGGER.info("Snapshot step 5 - Reading structure of captured tables");

            if (!readTableStructure(context, ctx)) {
                return SnapshotResult.aborted();
            }

            LOGGER.info("Snapshot step 6 - Persisting schema history");

            if (!createSchemaChangeEventsForTables(context, ctx)) {
                return SnapshotResult.aborted();
            }

            return SnapshotResult.completed(ctx.offset);
        }
        catch(RuntimeException e) {
            throw e;
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            rollbackTransaction(connection);

            LOGGER.info("Snapshot step 7 - Finalizing");

            complete();
        }
    }

    /**
     * Prepares the taking of a snapshot and returns an initial {@link SnapshotContext}.
     */
    protected abstract SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext) throws Exception;

    private void determineCapturedTables(SnapshotContext ctx) throws Exception {
        Set<TableId> allTableIds = getAllTableIds(ctx);

        Set<TableId> capturedTables = new HashSet<>();

        for (TableId tableId : allTableIds) {
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                capturedTables.add(tableId);
            }
            else {
                LOGGER.trace("Skipping table {} as it's not included in the filter configuration", tableId);
            }
        }

        ctx.capturedTables = capturedTables;
    }

    /**
     * Returns all candidate tables; the current filter configuration will be applied to the result set, resulting in
     * the effective set of captured tables.
     */
    protected abstract Set<TableId> getAllTableIds(SnapshotContext snapshotContext) throws Exception;

    /**
     * Locks all tables to be captured, so that no concurrent schema changes can be applied to them.
     */
    protected abstract boolean lockTables(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws Exception;

    /**
     * Determines the current offset (MySQL binlog position, Oracle SCN etc.), storing it into the passed context
     * object. Subsequently, the DB's schema (and data) will be be read at this position. Once the snapshot is
     * completed, a {@link StreamingChangeEventSource} will be set up with this initial position to continue with stream
     * reading from there.
     */
    protected abstract void determineSnapshotOffset(SnapshotContext snapshotContext) throws Exception;

    /**
     * Reads the structure of all the captured tables, writing it to {@link SnapshotContext#tables}.
     */
    protected abstract boolean readTableStructure(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws Exception;

    private boolean createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws Exception {
        for (TableId tableId : snapshotContext.capturedTables) {
            if (!sourceContext.isRunning()) {
                return false;
            }

            LOGGER.debug("Capturing structure of table {}", tableId);

            Table table = snapshotContext.tables.forTable(tableId);

            schema.applySchemaChange(getCreateTableEvent(snapshotContext, table));
        }

        return true;
    }

    /**
     * Creates a {@link SchemaChangeEvent} representing the creation of the given table.
     */
    protected abstract SchemaChangeEvent getCreateTableEvent(SnapshotContext snapshotContext, Table table) throws Exception;

    /**
     * Completes the snapshot, doing any required clean-up (resource disposal etc.).
     */
    protected abstract void complete();

    private void rollbackTransaction(Connection connection) {
        if(connection != null) {
            try {
                connection.rollback();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    public static class SnapshotContext implements AutoCloseable {

        public final String catalogName;
        public final Tables tables;

        public Set<TableId> capturedTables;
        public OffsetContext offset;

        public SnapshotContext(String catalogName) throws SQLException {
            this.catalogName = catalogName;
            this.tables = new Tables();
        }

        @Override
        public void close() throws Exception {
        }
    }
}
