/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * A utility that simplifies using a JDBC connection and executing transactions composed of multiple statements.
 * 
 * @author Randall Hauch
 */
public class JdbcConnection implements AutoCloseable {

    private final static Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);

    /**
     * Establishes JDBC connections.
     */
    @FunctionalInterface
    @ThreadSafe
    public static interface ConnectionFactory {
        /**
         * Establish a connection to the database denoted by the given configuration.
         * 
         * @param config the configuration with JDBC connection information
         * @return the JDBC connection; may not be null
         * @throws SQLException if there is an error connecting to the database
         */
        Connection connect(JdbcConfiguration config) throws SQLException;
    }

    /**
     * Defines multiple JDBC operations.
     */
    @FunctionalInterface
    public static interface Operations {
        /**
         * Apply a series of operations against the given JDBC statement.
         * 
         * @param statement the JDBC statement to use to execute one or more operations
         * @throws SQLException if there is an error connecting to the database or executing the statements
         */
        void apply(Statement statement) throws SQLException;
    }

    /**
     * Create a {@link ConnectionFactory} that replaces variables in the supplied URL pattern. Variables include:
     * <ul>
     * <li><code>${hostname}</code></li>
     * <li><code>${port}</code></li>
     * <li><code>${dbname}</code></li>
     * <li><code>${username}</code></li>
     * <li><code>${password}</code></li>
     * </ul>
     * 
     * @param urlPattern the URL pattern string; may not be null
     * @return the connection factory
     */
    protected static ConnectionFactory patternBasedFactory(String urlPattern) {
        return (config) -> {
            LOGGER.trace("Config: {}", config.asProperties());
            Properties props = config.asProperties();
            String url = findAndReplace(urlPattern, props,
                                        JdbcConfiguration.HOSTNAME,
                                        JdbcConfiguration.PORT,
                                        JdbcConfiguration.USER,
                                        JdbcConfiguration.PASSWORD,
                                        JdbcConfiguration.DATABASE);
            LOGGER.trace("Props: {}", props);
            LOGGER.trace("URL: {}", url);
            Connection conn = DriverManager.getConnection(url, props);
            LOGGER.debug("Connected to {} with {}", url, props);
            return conn;
        };
    }

    private static String findAndReplace(String url, Properties props, Configuration.Field... variables) {
        for (Configuration.Field field : variables ) {
            String variable = field.name();
            if (variable != null && url.contains("${" + variable + "}")) {
                // Otherwise, we have to remove it from the properties ...
                String value = props.getProperty(variable);
                if ( value != null ) {
                    props.remove(variable);
                    // And replace the variable ...
                    url = url.replaceAll("\\$\\{" + variable + "\\}", value);
                }
            }
        }
        return url;
    }

    private final Configuration config;
    private final ConnectionFactory factory;
    private final Operations initialOps;
    private volatile Connection conn;

    /**
     * Create a new instance with the given configuration and connection factory.
     * 
     * @param config the configuration; may not be null
     * @param connectionFactory the connection factory; may not be null
     */
    public JdbcConnection(Configuration config, ConnectionFactory connectionFactory) {
        this(config, connectionFactory, null);
    }

    /**
     * Create a new instance with the given configuration and connection factory, and specify the operations that should be
     * run against each newly-established connection.
     * 
     * @param config the configuration; may not be null
     * @param connectionFactory the connection factory; may not be null
     * @param initialOperations the initial operations that should be run on each new connection; may be null
     */
    public JdbcConnection(Configuration config, ConnectionFactory connectionFactory, Operations initialOperations) {
        this.config = config;
        this.factory = connectionFactory;
        this.initialOps = initialOperations;
        this.conn = null;
    }

    /**
     * Ensure a connection to the database is established.
     * 
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database
     */
    public JdbcConnection connect() throws SQLException {
        connection();
        return this;
    }

    /**
     * Execute a series of SQL statements as a single transaction.
     * 
     * @param sqlStatements the SQL statements that are to be performed as a single transaction
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection execute(String... sqlStatements) throws SQLException {
        return execute(statement -> {
            for (String sqlStatement : sqlStatements) {
                if (sqlStatement != null) statement.execute(sqlStatement);
            }
        });
    }

    /**
     * Execute a series of operations as a single transaction.
     * 
     * @param operations the function that will be called with a newly-created {@link Statement}, and that performs
     *            one or more operations on that statement object
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     */
    public JdbcConnection execute(Operations operations) throws SQLException {
        Connection conn = connection();
        conn.setAutoCommit(false);
        try (Statement statement = conn.createStatement();) {
            operations.apply(statement);
            conn.commit();
        }
        return this;
    }

    /**
     * Execute a SQL query.
     * 
     * @param query the SQL query
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection query(String query, Consumer<ResultSet> resultConsumer) throws SQLException {
        Connection conn = connection();
        conn.setAutoCommit(false);
        try (Statement statement = conn.createStatement();) {
            ResultSet resultSet = statement.executeQuery(query);
            if (resultConsumer != null) resultConsumer.accept(resultSet);
        }
        return this;
    }
    
    public void print(ResultSet resultSet ) {
        // CHECKSTYLE:OFF
        print(resultSet,System.out::println);
        // CHECKSTYLE:ON
    }
    
    public void print(ResultSet resultSet, Consumer<String> lines ) {
        try {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnCount = rsmd.getColumnCount();
            int[] columnSizes = findMaxLength(resultSet);
            lines.accept(delimiter(columnCount, columnSizes));
            StringBuilder sb = new StringBuilder();
            for ( int i=1; i<=columnCount; i++ ) {
                if (i > 1) sb.append(" | ");
                sb.append(Strings.setLength(rsmd.getColumnLabel(i),columnSizes[i],' '));
            }
            lines.accept(sb.toString());
            sb.setLength(0);
            lines.accept(delimiter(columnCount, columnSizes));
            while (resultSet.next()) {
                sb.setLength(0);
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) sb.append(" | ");
                    sb.append(Strings.setLength(resultSet.getString(i),columnSizes[i],' '));
                }
                lines.accept(sb.toString());
                sb.setLength(0);
            }
            lines.accept(delimiter(columnCount, columnSizes));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private String delimiter( int columnCount, int[] columnSizes ) {
        StringBuilder sb = new StringBuilder();
        for ( int i=1; i<=columnCount; i++ ) {
            if (i > 1) sb.append("---");
            sb.append(Strings.createString('-',columnSizes[i]));
        }
        return sb.toString();
    }
    
    private int[] findMaxLength( ResultSet resultSet ) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();
        int[] columnSizes = new int[columnCount+1];
        for ( int i=1; i<=columnCount; i++ ) {
            columnSizes[i] = Math.max(columnSizes[i], rsmd.getColumnLabel(i).length());
        }
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                String value = resultSet.getString(i);
                if ( value != null ) columnSizes[i] = Math.max(columnSizes[i], value.length());
            }
        }
        resultSet.beforeFirst();
        return columnSizes;
    }

    public synchronized Connection connection() throws SQLException {
        if (conn == null) {
            conn = factory.connect(JdbcConfiguration.adapt(config));
            if (conn == null) throw new SQLException("Unable to obtain a JDBC connection");
            // Always run the initial operations on this new connection
            if (initialOps != null) execute(initialOps);
        }
        return conn;
    }

    /**
     * Close the connection and release any resources.
     */
    @Override
    public synchronized void close() throws SQLException {
        if (conn != null) {
            try {
                conn.close();
            } finally {
                conn = null;
            }
        }
    }
    
    /**
     * Create definitions for each tables in the database, given the catalog name, schema pattern, table filter, and
     * column filter.
     * 
     * @param tables the set of table definitions to be modified; may not be null
     * @param databaseCatalog the name of the catalog, which is typically the database name; may be null if all accessible
     *            databases are to be processed
     * @param schemaNamePattern the pattern used to match database schema names, which may be "" to match only those tables with
     *            no schema or null to process all accessible tables regardless of database schema name
     * @param tableFilter used to determine for which tables are to be processed; may be null if all accessible tables are to be
     *            processed
     * @param columnFilter used to determine which columns should be included as fields in its table's definition; may
     *            be null if all columns for all tables are to be included
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern,
                           TableFilter tableFilter, ColumnFilter columnFilter) throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();

        // Read the metadata for the table columns ...
        ConcurrentMap<TableId, List<Column>> columnsByTable = new ConcurrentHashMap<>();
        try (ResultSet rs = metadata.getColumns(databaseCatalog, schemaNamePattern, null, null)) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tableName = rs.getString(3);
                if (tableFilter == null || tableFilter.test(catalogName, schemaName, tableName)) {
                    TableId tableId = new TableId(catalogName, schemaName, tableName);
                    List<Column> cols = columnsByTable.computeIfAbsent(tableId, name -> new ArrayList<>());
                    String columnName = rs.getString(4);
                    if (columnFilter == null || columnFilter.test(catalogName, schemaName, tableName, columnName)) {
                        ColumnEditor column = Column.editor().name(columnName);
                        column.jdbcType(rs.getInt(5));
                        column.typeName(rs.getString(6));
                        column.length(rs.getInt(7));
                        column.scale(rs.getInt(9));
                        column.optional(isNullable(rs.getInt(11)));
                        column.position(rs.getInt(17));
                        column.autoIncremented("YES".equalsIgnoreCase(rs.getString(23)));
                        column.generated("YES".equalsIgnoreCase(rs.getString(24)));
                        cols.add(column.create());
                    }
                }
            }
        }

        // Read the metadata for the primary keys ...
        for (TableId id : columnsByTable.keySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = null;
            try (ResultSet rs = metadata.getPrimaryKeys(id.catalog(), id.schema(), id.table())) {
                while (rs.next()) {
                    if (pkColumnNames == null) pkColumnNames = new ArrayList<>();
                    String columnName = rs.getString(4);
                    int columnIndex = rs.getInt(5);
                    Collect.set(pkColumnNames, columnIndex - 1, columnName, null);
                }
            }

            // Then define the table ...
            List<Column> columns = columnsByTable.get(id);
            Collections.sort(columns);
            tables.overwriteTable(id, columns, pkColumnNames);
        }
    }

    /**
     * Use the supplied table editor to create columns for the supplied result set.
     * 
     * @param resultSet the query result set; may not be null
     * @param editor the consumer of the definitions; may not be null
     * @throws SQLException if an error occurs while using the result set
     */
    public static void columnsFor(ResultSet resultSet, TableEditor editor) throws SQLException {
        List<Column> columns = new ArrayList<>();
        columnsFor(resultSet,columns::add);
        editor.setColumns(columns);
    }

    /**
     * Determine the column definitions for the supplied result set and add each column to the specified consumer.
     * 
     * @param resultSet the query result set; may not be null
     * @param consumer the consumer of the definitions; may not be null
     * @throws SQLException if an error occurs while using the result set
     */
    public static void columnsFor(ResultSet resultSet, Consumer<Column> consumer) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        ColumnEditor column = Column.editor();
        for (int position = 1; position <= metadata.getColumnCount(); ++position) {
            String columnLabel = metadata.getColumnLabel(position);
            column.name(columnLabel != null ? columnLabel : metadata.getColumnName(position));
            column.typeName(metadata.getColumnTypeName(position));
            column.jdbcType(metadata.getColumnType(position));
            column.length(metadata.getPrecision(position));
            column.scale(metadata.getScale(position));
            column.optional(isNullable(metadata.isNullable(position)));
            column.autoIncremented(metadata.isAutoIncrement(position));
            column.generated(false);
            consumer.accept(column.create());
        }
    }

    private static boolean isNullable(int jdbcNullable) {
        return jdbcNullable == ResultSetMetaData.columnNullable || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
    }



}
