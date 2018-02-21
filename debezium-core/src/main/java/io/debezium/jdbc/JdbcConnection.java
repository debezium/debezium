/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableNameFilter;
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
     * @param variables any custom or overridden configuration variables
     * @return the connection factory
     */
    public static ConnectionFactory patternBasedFactory(String urlPattern, Field... variables) {
        return (config) -> {
            LOGGER.trace("Config: {}", config.asProperties());
            Properties props = config.asProperties();
            Field[] varsWithDefaults = combineVariables(variables,
                                                        JdbcConfiguration.HOSTNAME,
                                                        JdbcConfiguration.PORT,
                                                        JdbcConfiguration.USER,
                                                        JdbcConfiguration.PASSWORD,
                                                        JdbcConfiguration.DATABASE);
            String url = findAndReplace(urlPattern, props, varsWithDefaults);
            LOGGER.trace("Props: {}", props);
            LOGGER.trace("URL: {}", url);
            Connection conn = DriverManager.getConnection(url, props);
            LOGGER.debug("Connected to {} with {}", url, props);
            return conn;
        };
    }

    /**
     * Create a {@link ConnectionFactory} that uses the specific JDBC driver class loaded with the given class loader, and obtains the connection URL by replacing the following variables in the URL pattern:
     * <ul>
     * <li><code>${hostname}</code></li>
     * <li><code>${port}</code></li>
     * <li><code>${dbname}</code></li>
     * <li><code>${username}</code></li>
     * <li><code>${password}</code></li>
     * </ul>
     * <p>
     * This method attempts to instantiate the JDBC driver class and use that instance to connect to the database.
     * @param urlPattern the URL pattern string; may not be null
     * @param driverClassName the name of the JDBC driver class; may not be null
     * @param classloader the ClassLoader that should be used to load the JDBC driver class given by `driverClassName`; may be null if this class' class loader should be used
     * @param variables any custom or overridden configuration variables
     * @return the connection factory
     */
    @SuppressWarnings("unchecked")
    public static ConnectionFactory patternBasedFactory(String urlPattern, String driverClassName,
            ClassLoader classloader, Field... variables) {
        return (config) -> {
            LOGGER.trace("Config: {}", config.asProperties());
            Properties props = config.asProperties();
            Field[] varsWithDefaults = combineVariables(variables,
                                                        JdbcConfiguration.HOSTNAME,
                                                        JdbcConfiguration.PORT,
                                                        JdbcConfiguration.USER,
                                                        JdbcConfiguration.PASSWORD,
                                                        JdbcConfiguration.DATABASE);
            String url = findAndReplace(urlPattern, props, varsWithDefaults);
            LOGGER.trace("Props: {}", props);
            LOGGER.trace("URL: {}", url);
            Connection conn = null;
            try {
                ClassLoader driverClassLoader = classloader;
                if (driverClassLoader == null) {
                    driverClassLoader = JdbcConnection.class.getClassLoader();
                }
                Class<java.sql.Driver> driverClazz = (Class<java.sql.Driver>) Class.forName(driverClassName, true, driverClassLoader);
                java.sql.Driver driver = driverClazz.newInstance();
                conn = driver.connect(url, props);
            } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
                throw new SQLException(e);
            }
            LOGGER.debug("Connected to {} with {}", url, props);
            return conn;
        };
    }

    private static Field[] combineVariables(Field[] overriddenVariables,
                                            Field... defaultVariables) {
        Map<String, Field> fields = new HashMap<>();
        if (defaultVariables != null) {
            for (Field variable : defaultVariables) {
                fields.put(variable.name(), variable);
            }
        }
        if (overriddenVariables != null) {
            for (Field variable : overriddenVariables) {
                fields.put(variable.name(), variable);
            }
        }
        return fields.values().toArray(new Field[fields.size()]);
    }

    private static String findAndReplace(String url, Properties props, Field... variables) {
        for (Field field : variables) {
            if ( field != null ) url = findAndReplace(url, field.name(), props);
        }
        for (Object key : new HashSet<>(props.keySet())) {
            if (key != null ) url = findAndReplace(url, key.toString(), props);
        }
        return url;
    }

    private static String findAndReplace(String url, String name, Properties props) {
        if (name != null && url.contains("${" + name + "}")) {
            // Otherwise, we have to remove it from the properties ...
            String value = props.getProperty(name);
            if (value != null) {
                props.remove(name);
                // And replace the variable ...
                url = url.replaceAll("\\$\\{" + name + "\\}", value);
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
        this(config, connectionFactory, initialOperations, null);
    }

    /**
     * Create a new instance with the given configuration and connection factory, and specify the operations that should be
     * run against each newly-established connection.
     *
     * @param config the configuration; may not be null
     * @param connectionFactory the connection factory; may not be null
     * @param initialOperations the initial operations that should be run on each new connection; may be null
     * @param adapter the function that can be called to update the configuration with defaults
     */
    protected JdbcConnection(Configuration config, ConnectionFactory connectionFactory, Operations initialOperations,
            Consumer<Configuration.Builder> adapter) {
        this.config = adapter == null ? config : config.edit().apply(adapter).build();
        this.factory = connectionFactory;
        this.initialOps = initialOperations;
        this.conn = null;
    }

    /**
     * Obtain the configuration for this connection.
     *
     * @return the JDBC configuration; never null
     */
    public JdbcConfiguration config() {
        return JdbcConfiguration.adapt(config);
    }

    public JdbcConnection setAutoCommit(boolean autoCommit) throws SQLException {
        connection().setAutoCommit(autoCommit);
        return this;
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
                if (sqlStatement != null) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("executing '{}'", sqlStatement);
                    }
                    statement.execute(sqlStatement);
                }
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
        try (Statement statement = conn.createStatement();) {
            operations.apply(statement);
            if (!conn.getAutoCommit()) conn.commit();
        }
        return this;
    }

    public static interface ResultSetConsumer {
        void accept(ResultSet rs) throws SQLException;
    }

    public static interface BlockingResultSetConsumer {
        void accept(ResultSet rs) throws SQLException, InterruptedException;
    }

    public static interface SingleParameterResultSetConsumer {
        boolean accept(String parameter, ResultSet rs) throws SQLException;
    }

    public static interface StatementPreparer {
        void accept(PreparedStatement statement) throws SQLException;
    }

    @FunctionalInterface
    public static interface CallPreparer {
        void accept(CallableStatement statement) throws SQLException;
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
    public JdbcConnection query(String query, ResultSetConsumer resultConsumer) throws SQLException {
        return query(query, Connection::createStatement, resultConsumer);
    }

    /**
     * Execute a stored procedure.
     *
     * @param sql the SQL query; may not be {@code null}
     * @param callPreparer a {@link CallPreparer} instance which can be used to set additional parameters; may be null
     * @param resultSetConsumer a {@link ResultSetConsumer} instance which can be used to process the results; may be null
     * @return this object for chaining methods together
     * @throws SQLException if anything unexpected fails
     */
    public JdbcConnection call(String sql, CallPreparer callPreparer, ResultSetConsumer resultSetConsumer) throws SQLException {
        Connection conn = connection();
        try (CallableStatement callableStatement = conn.prepareCall(sql)) {
            if (callPreparer != null) {
                callPreparer.accept(callableStatement);
            }
            try (ResultSet rs = callableStatement.executeQuery()) {
                if (resultSetConsumer != null) {
                    resultSetConsumer.accept(rs);
                }
            }
        }
        return this;
    }

    /**
     * Execute a SQL query.
     *
     * @param query the SQL query
     * @param statementFactory the function that should be used to create the statement from the connection; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection query(String query, StatementFactory statementFactory, ResultSetConsumer resultConsumer) throws SQLException {
        Connection conn = connection();
        try (Statement statement = statementFactory.createStatement(conn);) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("running '{}'", query);
            }
            try (ResultSet resultSet = statement.executeQuery(query);) {
                if (resultConsumer != null) {
                    resultConsumer.accept(resultSet);
                }
            }
        }
        return this;
    }

    public JdbcConnection queryWithBlockingConsumer(String query, StatementFactory statementFactory, BlockingResultSetConsumer resultConsumer) throws SQLException, InterruptedException {
        Connection conn = connection();
        try (Statement statement = statementFactory.createStatement(conn);) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("running '{}'", query);
            }
            try (ResultSet resultSet = statement.executeQuery(query);) {
                if (resultConsumer != null) {
                    resultConsumer.accept(resultSet);
                }
            }
        }
        return this;
    }

    /**
     * A function to create a statement from a connection.
     * @author Randall Hauch
     */
    @FunctionalInterface
    public interface StatementFactory {
        /**
         * Use the given connection to create a statement.
         * @param connection the JDBC connection; never null
         * @return the statement
         * @throws SQLException if there are problems creating a statement
         */
        Statement createStatement(Connection connection) throws SQLException;
    }

    /**
     * Execute a SQL prepared query.
     *
     * @param preparedQueryString the prepared query string
     * @param preparer the function that supplied arguments to the prepared statement; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQuery(String preparedQueryString, StatementPreparer preparer, ResultSetConsumer resultConsumer)
            throws SQLException {
        Connection conn = connection();
        try (PreparedStatement statement = conn.prepareStatement(preparedQueryString);) {
            preparer.accept(statement);
            try (ResultSet resultSet = statement.executeQuery();) {
                if (resultConsumer != null) resultConsumer.accept(resultSet);
            }
        }
        return this;
    }

    /**
     * Execute a SQL update via a prepared statement.
     *
     * @param stmt the statement string
     * @param preparer the function that supplied arguments to the prepared stmt; may be null
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareUpdate(String stmt, StatementPreparer preparer) throws SQLException {
        Connection conn = connection();
        try (PreparedStatement statement = conn.prepareStatement(stmt);) {
            if (preparer != null) {
                preparer.accept(statement);
            }
            statement.execute();
        }
        return this;
    }

    /**
     * Execute a SQL prepared query.
     *
     * @param preparedQueryString the prepared query string
     * @param parameters the collection of values for the first and only parameter in the query; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQuery(String preparedQueryString, Collection<String> parameters,
                                       SingleParameterResultSetConsumer resultConsumer)
            throws SQLException {
        return prepareQuery(preparedQueryString, parameters.stream(), resultConsumer);
    }

    /**
     * Execute a SQL prepared query.
     *
     * @param preparedQueryString the prepared query string
     * @param parameters the stream of values for the first and only parameter in the query; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQuery(String preparedQueryString, Stream<String> parameters,
                                       SingleParameterResultSetConsumer resultConsumer)
            throws SQLException {
        Connection conn = connection();
        try (PreparedStatement statement = conn.prepareStatement(preparedQueryString);) {
            for (Iterator<String> iter = parameters.iterator(); iter.hasNext();) {
                String value = iter.next();
                statement.setString(1, value);
                boolean success = false;
                try (ResultSet resultSet = statement.executeQuery();) {
                    if (resultConsumer != null) {
                        success = resultConsumer.accept(value, resultSet);
                        if (!success) break;
                    }
                }
            }
        }
        return this;
    }

    public void print(ResultSet resultSet) {
        // CHECKSTYLE:OFF
        print(resultSet, System.out::println);
        // CHECKSTYLE:ON
    }

    public void print(ResultSet resultSet, Consumer<String> lines) {
        try {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnCount = rsmd.getColumnCount();
            int[] columnSizes = findMaxLength(resultSet);
            lines.accept(delimiter(columnCount, columnSizes));
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) sb.append(" | ");
                sb.append(Strings.setLength(rsmd.getColumnLabel(i), columnSizes[i], ' '));
            }
            lines.accept(sb.toString());
            sb.setLength(0);
            lines.accept(delimiter(columnCount, columnSizes));
            while (resultSet.next()) {
                sb.setLength(0);
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) sb.append(" | ");
                    sb.append(Strings.setLength(resultSet.getString(i), columnSizes[i], ' '));
                }
                lines.accept(sb.toString());
                sb.setLength(0);
            }
            lines.accept(delimiter(columnCount, columnSizes));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String delimiter(int columnCount, int[] columnSizes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) sb.append("---");
            sb.append(Strings.createString('-', columnSizes[i]));
        }
        return sb.toString();
    }

    private int[] findMaxLength(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();
        int[] columnSizes = new int[columnCount + 1];
        for (int i = 1; i <= columnCount; i++) {
            columnSizes[i] = Math.max(columnSizes[i], rsmd.getColumnLabel(i).length());
        }
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                String value = resultSet.getString(i);
                if (value != null) columnSizes[i] = Math.max(columnSizes[i], value.length());
            }
        }
        resultSet.beforeFirst();
        return columnSizes;
    }

    public synchronized boolean isConnected() throws SQLException {
        if (conn == null) return false;
        return !conn.isClosed();
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
     * Get the names of all of the catalogs.
     * @return the set of catalog names; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<String> readAllCatalogNames()
            throws SQLException {
        Set<String> catalogs = new HashSet<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getCatalogs()) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                catalogs.add(catalogName);
            }
        }
        return catalogs;
    }

    /**
     * Get the names of all of the schemas, optionally applying a filter.
     *
     * @param filter a {@link Predicate} to test each schema name; may be null in which case all schema names are returned
     * @return the set of catalog names; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<String> readAllSchemaNames(Predicate<String> filter)
            throws SQLException {
        Set<String> schemas = new HashSet<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getSchemas()) {
            while (rs.next()) {
                String schema = rs.getString(1);
                if (filter != null && filter.test(schema)) {
                    schemas.add(schema);
                }
            }
        }
        return schemas;
    }

    public String[] tableTypes() throws SQLException {
        List<String> types = new ArrayList<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getTableTypes()) {
            while (rs.next()) {
                String tableType = rs.getString(1);
                if (tableType != null) types.add(tableType);
            }
        }
        return types.toArray(new String[types.size()]);
    }

    /**
     * Get the identifiers of all available tables.
     *
     * @param tableTypes the set of table types to include in the results, which may be null for all table types
     * @return the set of {@link TableId}s; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<TableId> readAllTableNames(String[] tableTypes) throws SQLException {
        return readTableNames(null, null, null, tableTypes);
    }

    /**
     * Get the identifiers of the tables.
     *
     * @param databaseCatalog the name of the catalog, which is typically the database name; may be an empty string for tables
     *            that have no catalog, or {@code null} if the catalog name should not be used to narrow the list of table
     *            identifiers
     * @param schemaNamePattern the pattern used to match database schema names, which may be "" to match only those tables with
     *            no schema or {@code null} if the schema name should not be used to narrow the list of table
     *            identifiers
     * @param tableNamePattern the pattern used to match database table names, which may be null to match all table names
     * @param tableTypes the set of table types to include in the results, which may be null for all table types
     * @return the set of {@link TableId}s; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
                                       String[] tableTypes)
            throws SQLException {
        if (tableNamePattern == null) tableNamePattern = "%";
        Set<TableId> tableIds = new HashSet<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getTables(databaseCatalog, schemaNamePattern, tableNamePattern, tableTypes)) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tableName = rs.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, tableName);
                tableIds.add(tableId);
            }
        }
        return tableIds;
    }

    /**
     * Returns a JDBC connection string using the current configuration and url.
     *
     * @param urlPattern a {@code String} representing a JDBC connection with variables that will be replaced
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString(String urlPattern) {
        Properties props = config.asProperties();
        return findAndReplace(urlPattern, props, JdbcConfiguration.DATABASE, JdbcConfiguration.HOSTNAME, JdbcConfiguration.PORT,
                              JdbcConfiguration.USER, JdbcConfiguration.PASSWORD);
    }

    /**
     * Returns the username for this connection
     *
     * @return a {@code String}, never {@code null}
     */
    public String username()  {
        return config.getString(JdbcConfiguration.USER);
    }
  /**
     * Returns the database name for this connection
     *
     * @return a {@code String}, never {@code null}
     */
    public String database()  {
        return config.getString(JdbcConfiguration.DATABASE);
    }

    /**
     * Provides a native type for the given type name.
     *
     * There isn't a standard way to obtain this information via JDBC APIs so this method exists to allow
     * database specific information to be set in addition to the JDBC Type.
     *
     * @param typeName the name of the type whose native type we are looking for
     * @return A type constant for the specific database or -1.
     */
    protected int resolveNativeType(String typeName) {
        return Column.UNSET_INT_VALUE;
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
     * @param removeTablesNotFoundInJdbc {@code true} if this method should remove from {@code tables} any definitions for tables
     *            that are not found in the database metadata, or {@code false} if such tables should be left untouched
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern,
                           TableNameFilter tableFilter, ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc)
            throws SQLException {
        // Before we make any changes, get the copy of the set of table IDs ...
        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        // Read the metadata for the table columns ...
        DatabaseMetaData metadata = connection().getMetaData();

        // Find views as they cannot be snapshotted
        final Set<TableId> viewIds = new HashSet<>();
        try (final ResultSet rs = metadata.getTables(databaseCatalog, schemaNamePattern, null, new String[] {"VIEW"})) {
            while (rs.next()) {
                final String catalogName = rs.getString(1);
                final String schemaName = rs.getString(2);
                final String tableName = rs.getString(3);
                viewIds.add(new TableId(catalogName, schemaName, tableName));
            }
        }

        ConcurrentMap<TableId, List<Column>> columnsByTable = new ConcurrentHashMap<>();
        try (ResultSet rs = metadata.getColumns(databaseCatalog, schemaNamePattern, null, null)) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tableName = rs.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, tableName);
                if (viewIds.contains(tableId)) {
                    continue;
                }
                if (tableFilter == null || tableFilter.matches(catalogName, schemaName, tableName)) {
                    List<Column> cols = columnsByTable.computeIfAbsent(tableId, name -> new ArrayList<>());
                    String columnName = rs.getString(4);
                    if (columnFilter == null || columnFilter.matches(catalogName, schemaName, tableName, columnName)) {
                        ColumnEditor column = Column.editor().name(columnName);
                        column.jdbcType(rs.getInt(5));
                        column.type(rs.getString(6));
                        column.length(rs.getInt(7));
                        column.scale(rs.getInt(9));
                        column.optional(isNullable(rs.getInt(11)));
                        column.position(rs.getInt(17));
                        column.autoIncremented("YES".equalsIgnoreCase(rs.getString(23)));
                        String autogenerated = null;
                        try {
                            autogenerated = rs.getString(24);
                        } catch (SQLException e) {
                            // ignore, some drivers don't have this index - e.g. Postgres
                        }
                        column.generated("YES".equalsIgnoreCase(autogenerated));

                        column.nativeType(resolveNativeType(column.typeName()));

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
            String defaultCharsetName = null; // JDBC does not expose character sets
            tables.overwriteTable(id, columns, pkColumnNames, defaultCharsetName);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }
    }

    /**
     * Returns a map which contains information about how a database maps it's data types to JDBC data types.
     *
     * @return a {@link Map map of (localTypeName, jdbcType) pairs}
     * @throws SQLException if anything unexpected fails
     */
    public Map<String, Integer> readTypeInfo() throws SQLException {
        DatabaseMetaData metadata = connection().getMetaData();
        Map<String, Integer> jdbcTypeByLocalTypeName = new HashMap<>();
        try (ResultSet rs = metadata.getTypeInfo()) {
           while (rs.next()) {
               jdbcTypeByLocalTypeName.put(rs.getString("TYPE_NAME"), rs.getInt("DATA_TYPE"));
           }
        }
        return jdbcTypeByLocalTypeName;
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
        columnsFor(resultSet, columns::add);
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
            column.type(metadata.getColumnTypeName(position));
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
