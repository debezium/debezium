/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.HistorizedRelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;

public class OracleSnapshotChangeEventSource extends HistorizedRelationalSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSnapshotChangeEventSource.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;

    public OracleSnapshotChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext previousOffset, OracleConnection jdbcConnection, OracleDatabaseSchema schema) {
        super(connectorConfig, previousOffset, jdbcConnection, schema);

        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext context) throws Exception {
        if (connectorConfig.getPdbName() != null) {
            jdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
        }

        return new OracleSnapshotContext(
                connectorConfig.getPdbName() != null ? connectorConfig.getPdbName() : connectorConfig.getDatabaseName()
        );
    }

    @Override
    protected Set<TableId> getAllTableIds(SnapshotContext ctx) throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"} );
    }

    @Override
    protected boolean lockTables(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws SQLException {
        try (Statement statement = jdbcConnection.connection().createStatement()) {
            for (TableId tableId : snapshotContext.capturedTables) {
                if (!sourceContext.isRunning()) {
                    return false;
                }

                LOGGER.debug("Locking table {}", tableId);

                statement.execute("LOCK TABLE " + tableId.schema() + "." + tableId.table() + " IN EXCLUSIVE MODE");
            }
        }

        return true;
    }

    @Override
    protected void determineSnapshotOffset(SnapshotContext ctx) throws Exception {
        try(Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery("select CURRENT_SCN from V$DATABASE") ) {

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }

            Long scn = rs.getLong(1);

            OracleOffsetContext offset = new OracleOffsetContext(connectorConfig.getLogicalName());
            offset.setScn(scn);

            ctx.offset = offset;
        }
    }

    @Override
    protected boolean readTableStructure(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws SQLException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
            .map(TableId::schema)
            .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                return false;
            }

            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false
            );
        }

        return true;
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(SnapshotContext snapshotContext, Table table) throws SQLException {
        try (Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery("select dbms_metadata.get_ddl( 'TABLE', '" + table.id().table() + "', '" +  table.id().schema() + "' ) from dual")) {

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get metadata");
            }

            Object res = rs.getObject(1);
            String ddl = ((Clob)res).getSubString(1, (int) ((Clob)res).length());

            return new SchemaChangeEvent(snapshotContext.offset.getPartition(), snapshotContext.offset.getOffset(), snapshotContext.catalogName,
                    table.id().schema(), ddl, table, SchemaChangeEventType.CREATE, true);
        }
    }

    @Override
    protected void complete() {
        if (connectorConfig.getPdbName() != null) {
            jdbcConnection.resetSessionToCdb();
        }
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class OracleSnapshotContext extends SnapshotContext {

        public OracleSnapshotContext(String catalogName) throws SQLException {
            super(catalogName);
        }
    }
}
