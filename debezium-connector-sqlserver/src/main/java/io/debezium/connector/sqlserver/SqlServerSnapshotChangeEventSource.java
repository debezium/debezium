/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;

public class SqlServerSnapshotChangeEventSource implements SnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSnapshotChangeEventSource.class);

    private final SqlServerConnectorConfig connectorConfig;
    private final SqlServerOffsetContext previousOffset;
    private final SqlServerConnection jdbcConnection;
    private final SqlServerDatabaseSchema schema;

    public SqlServerSnapshotChangeEventSource(SqlServerConnectorConfig connectorConfig, SqlServerOffsetContext previousOffset, SqlServerConnection jdbcConnection, SqlServerDatabaseSchema schema) {
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
        SnapshotContext ctx = null;

        try {
            connection = jdbcConnection.connection();
            connection.setAutoCommit(false);

            ctx = new SnapshotContext(
                    context,
                    connection,
                    connectorConfig.getDatabaseName()
            );

            ctx.capturedTables = schema.getCapturedTables();

            if (!lockDatabase(ctx)) {
                return SnapshotResult.aborted();
            }

            determineOffsetContextWithLsn(ctx);
            readTableStructure(ctx);

            if (!createSchemaChangeEventsForTables(ctx)) {
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
            if (ctx != null) {
                ctx.dispose();
            }

            rollbackTransaction(connection);
        }
    }

    private boolean lockDatabase(SnapshotContext ctx) throws SQLException {
        // TODO use SET SINGLE
        return true;
    }

    private void determineOffsetContextWithLsn(SnapshotContext ctx) throws SQLException {
        ctx.offset = new SqlServerOffsetContext(connectorConfig.getLogicalName());
        final Lsn lsn = jdbcConnection.getMaxLsn();
    }

    private void readTableStructure(SnapshotContext ctx) throws SQLException {
        ctx.tables = new Tables();

        Set<String> schemas = ctx.capturedTables.stream()
            .map(TableId::schema)
            .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            jdbcConnection.readSchema(
                    ctx.tables,
                    ctx.catalogName,
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false
            );
        }
    }

    private boolean createSchemaChangeEventsForTables(SnapshotContext ctx) throws SQLException {
        for (TableId tableId : ctx.capturedTables) {
            if (!ctx.changeEventSourceContext.isRunning()) {
                return false;
            }

            LOGGER.debug("Capturing structure of table {}", tableId);
            Table table = ctx.tables.forTable(tableId);

            // TODO - use sp_help and sp_columns to build CREATE TABLE
            final String ddl = "";

            schema.applySchemaChange(new SchemaChangeEvent(ctx.offset.getPartition(), ctx.offset.getOffset(), ctx.catalogName,
                    tableId.schema(), ddl, table, SchemaChangeEventType.CREATE, true));
        }

        return true;
    }

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
    private static class SnapshotContext {

        public final ChangeEventSourceContext changeEventSourceContext;
        public final Statement statement;
        public final String catalogName;

        public Set<TableId> capturedTables;
        public SqlServerOffsetContext offset;
        public Tables tables;

        public SnapshotContext(ChangeEventSourceContext changeEventSourceContext, Connection connection, String catalogName) throws SQLException {
            this.changeEventSourceContext = changeEventSourceContext;
            this.statement = connection.createStatement();
            this.catalogName = catalogName;
        }

        public void dispose() {
            try {
                statement.close();
            }
            catch (SQLException e) {
                LOGGER.error("Couldn't close statement", e);
            }
        }
    }
}
