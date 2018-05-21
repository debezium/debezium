/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableNameFilter;

public class OracleConnection extends JdbcConnection {

    private final static Logger LOGGER = LoggerFactory.getLogger(OracleConnection.class);

    public OracleConnection(Configuration config, ConnectionFactory connectionFactory) {
        super(config, connectionFactory);
    }

    public void setSessionToPdb(String pdbName) {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=" + pdbName);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    public void resetSessionToCdb() {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=cdb$root");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    @Override
    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
            String[] tableTypes) throws SQLException {

        Set<TableId> tableIds = super.readTableNames(null, schemaNamePattern, tableNamePattern, tableTypes);

        return tableIds.stream()
                .map(t -> new TableId(databaseCatalog, t.schema(), t.table()))
                .collect(Collectors.toSet());
    }

    @Override
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern, TableNameFilter tableFilter,
            ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc) throws SQLException {

        super.readSchema(tables, null, schemaNamePattern, tableFilter, columnFilter, removeTablesNotFoundInJdbc);

        Set<TableId> tableIds = new HashSet<>(tables.tableIds());

        for (TableId tableId : tableIds) {
            TableEditor editor = tables.editTable(tableId);
            editor.tableId(new TableId(databaseCatalog, tableId.schema(), tableId.table()));

            List<String> columnNames = new ArrayList<>(editor.columnNames());
            for (String columnName : columnNames) {
                Column column = editor.columnWithName(columnName);
                if (column.jdbcType() == Types.TIMESTAMP) {
                    editor.addColumn(
                            column.edit()
                                .length(column.scale())
                                .scale(-1)
                                .create()
                            );
                }
            }
            tables.overwriteTable(editor.create());
            tables.removeTable(tableId);
        }
    }
}
