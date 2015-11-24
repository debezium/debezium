/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
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
                                        JdbcConfiguration.Field.HOSTNAME,
                                        JdbcConfiguration.Field.PORT,
                                        JdbcConfiguration.Field.USER,
                                        JdbcConfiguration.Field.PASSWORD,
                                        JdbcConfiguration.Field.DATABASE);
            LOGGER.trace("Props: {}", props);
            LOGGER.trace("URL: {}", url);
            Connection conn = DriverManager.getConnection(url, props);
            LOGGER.debug("Connected to {} with {}", url, props);
            return conn;
        };
    }

    private static String findAndReplace(String url, Properties props, String... variableNames) {
        for (String variable : variableNames) {
            if (url.contains("${" + variable + "}")) {
                // Otherwise, we have to remove it from the properties ...
                String value = props.getProperty(variable);
                props.remove(variable);
                // And replace the variable ...
                url = url.replaceAll("\\$\\{" + variable + "\\}", value);
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

    protected synchronized Connection connection() throws SQLException {
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

}
