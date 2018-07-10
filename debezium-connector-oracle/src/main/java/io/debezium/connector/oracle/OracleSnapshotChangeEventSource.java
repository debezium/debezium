/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
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

public class OracleSnapshotChangeEventSource implements SnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSnapshotChangeEventSource.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleOffsetContext previousOffset;
    private final OracleConnection jdbcConnection;
    private final OracleDatabaseSchema schema;

    public OracleSnapshotChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext previousOffset, OracleConnection jdbcConnection, OracleDatabaseSchema schema) {
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

            if (connectorConfig.getPdbName() != null) {
                jdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
            }

            ctx = new SnapshotContext(
                    context,
                    connection,
                    connectorConfig.getPdbName() != null ? connectorConfig.getPdbName() : connectorConfig.getDatabaseName()
            );

            determineCapturedTables(ctx);

            if (!lockTablesToBeCaptured(ctx)) {
                return SnapshotResult.aborted();
            }

            determineOffsetContextWithScn(ctx);
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

            if (connectorConfig.getPdbName() != null) {
                jdbcConnection.resetSessionToCdb();
            }
        }
    }

    private void determineCapturedTables(SnapshotContext ctx) throws SQLException {
        Set<TableId> allTableIds = jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"} );

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

    private boolean lockTablesToBeCaptured(SnapshotContext ctx) throws SQLException {
        for (TableId tableId : ctx.capturedTables) {
            if (!ctx.changeEventSourceContext.isRunning()) {
                return false;
            }

            LOGGER.debug("Locking table {}", tableId);
            ctx.statement.execute("LOCK TABLE " + tableId.schema() + "." + tableId.table() + " IN EXCLUSIVE MODE");
        }

        return true;
    }

    private void determineOffsetContextWithScn(SnapshotContext ctx) throws SQLException {
        try (ResultSet rs = ctx.statement.executeQuery("select CURRENT_SCN from V$DATABASE")) {

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }

            Long scn = rs.getLong(1);

            ctx.offset = new OracleOffsetContext(connectorConfig.getLogicalName());
            ctx.offset.setScn(scn);
        }
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
                    (catalog, schemaName, tableName) -> {
                        return connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(new TableId(ctx.catalogName, schemaName, tableName));
                    },
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

            try (ResultSet rs = ctx.statement.executeQuery("select dbms_metadata.get_ddl( 'TABLE', '" + tableId.table() + "', '" +  tableId.schema() + "' ) from dual")) {
                if (!rs.next()) {
                    throw new IllegalStateException("Couldn't get metadata");
                }

                Object res = rs.getObject(1);
                String ddl = ((Clob)res).getSubString(1, (int) ((Clob)res).length());

                schema.applySchemaChange(new SchemaChangeEvent(ctx.offset.getPartition(), ctx.offset.getOffset(), ctx.catalogName,
                        tableId.schema(), ddl, table, SchemaChangeEventType.CREATE, true));
            }
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
        public OracleOffsetContext offset;
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
