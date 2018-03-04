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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;

public class OracleSnapshotChangeEventSource implements SnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSnapshotChangeEventSource.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;
    private final DatabaseSchema schema;

    public OracleSnapshotChangeEventSource(OracleConnectorConfig connectorConfig, OracleConnection jdbcConnection, DatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.schema = schema;
    }

    @Override
    public SnapshotResult execute(ChangeEventSourceContext context) throws InterruptedException {
        Connection connection = null;

        try {
            connection = jdbcConnection.connection();
            connection.setAutoCommit(false);

            Statement statement = connection.createStatement();

            if (connectorConfig.getPdbName() != null) {
                jdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
            }

            String catalogName = connectorConfig.getPdbName() != null ? connectorConfig.getPdbName() : connectorConfig.getDatabaseName();

            Set<TableId> tableNames = jdbcConnection.readTableNames(catalogName, "%DEBEZIUM%", null, new String[] {"TABLE"} );

            for (TableId tableId : tableNames) {
                if (!context.isRunning()) {
                    return SnapshotResult.aborted();
                }

                LOGGER.debug("Locking table {}", tableId);

                statement.execute("LOCK TABLE " + tableId.schema() + "." + tableId.table() + " IN EXCLUSIVE MODE");
            }

            ResultSet rs = statement.executeQuery("select DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER from dual");
            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }
            Long scn = rs.getLong(1);
            rs.close();

            Tables tables = new Tables();
            jdbcConnection.readSchema(tables, catalogName, "%DEBEZIUM%", null, null, false);

            for (TableId tableId : tableNames) {
                if (!context.isRunning()) {
                    return SnapshotResult.aborted();
                }

                LOGGER.debug("Capturing structure of table {}", tableId);

                Table table = tables.forTable(tableId);

                rs = statement.executeQuery("select dbms_metadata.get_ddl( 'TABLE', '" + tableId.table() + "', '" +  tableId.schema() + "' ) from dual");
                if (!rs.next()) {
                    throw new IllegalStateException("Couldn't get metadata");
                }
                Object res = rs.getObject(1);
                String ddl = ((Clob)res).getSubString(1, (int) ((Clob)res).length());
                rs.close();

                schema.applySchemaChange(new SchemaChangeEvent(ddl, table, SchemaChangeEventType.CREATE));
            }

            OracleOffsetContext offset = new OracleOffsetContext(connectorConfig.getLogicalName());
            offset.setScn(scn);

            return SnapshotResult.completed(offset);
        }
        catch(SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            rollbackTransaction(connection);

            if (connectorConfig.getPdbName() != null) {
                jdbcConnection.resetSessionToCdb();
            }
        }
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
}
