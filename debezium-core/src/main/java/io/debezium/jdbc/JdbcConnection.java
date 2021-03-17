/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.lang.reflect.InvocationTargetException;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.BoundedConcurrentHashMap.Eviction;
import io.debezium.util.BoundedConcurrentHashMap.EvictionListener;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * A utility that simplifies using a JDBC connection and executing transactions composed of multiple statements.
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public class JdbcConnection implements AutoCloseable {

    private static final int WAIT_FOR_CLOSE_SECONDS = 10;
    private static final char STATEMENT_DELIMITER = ';';
    private static final int STATEMENT_CACHE_CAPACITY = 10_000;
    private final static Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);
    private final Map<String, PreparedStatement> statementCache = new BoundedConcurrentHashMap<>(STATEMENT_CACHE_CAPACITY, 16, Eviction.LIRS,
            new EvictionListener<String, PreparedStatement>() {

                @Override
                public void onEntryEviction(Map<String, PreparedStatement> evicted) {
                }

                @Override
                public void onEntryChosenForEviction(PreparedStatement statement) {
                    cleanupPreparedStatement(statement);
                }
            });

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

    private class ConnectionFactoryDecorator implements ConnectionFactory {
        private final ConnectionFactory defaultConnectionFactory;
        private final Supplier<ClassLoader> classLoaderSupplier;
        private ConnectionFactory customConnectionFactory;

        private ConnectionFactoryDecorator(ConnectionFactory connectionFactory, Supplier<ClassLoader> classLoaderSupplier) {
            this.defaultConnectionFactory = connectionFactory;
            this.classLoaderSupplier = classLoaderSupplier;
        }

        @Override
        public Connection connect(JdbcConfiguration config) throws SQLException {
            if (Strings.isNullOrEmpty(config.getConnectionFactoryClassName())) {
                return defaultConnectionFactory.connect(config);
            }
            if (customConnectionFactory == null) {
                customConnectionFactory = config.getInstance(JdbcConfiguration.CONNECTION_FACTORY_CLASS,
                        ConnectionFactory.class, classLoaderSupplier);
            }
            return customConnectionFactory.connect(config);
        }
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
     * Extracts a data of resultset..
     */
    @FunctionalInterface
    public static interface ResultSetExtractor<T> {
        T apply(ResultSet rs) throws SQLException;
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
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Config: {}", propsWithMaskedPassword(config.asProperties()));
            }
            Properties props = config.asProperties();
            Field[] varsWithDefaults = combineVariables(variables,
                    JdbcConfiguration.HOSTNAME,
                    JdbcConfiguration.PORT,
                    JdbcConfiguration.USER,
                    JdbcConfiguration.PASSWORD,
                    JdbcConfiguration.DATABASE);
            String url = findAndReplace(urlPattern, props, varsWithDefaults);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Props: {}", propsWithMaskedPassword(props));
            }
            LOGGER.trace("URL: {}", url);
            Connection conn = DriverManager.getConnection(url, props);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Connected to {} with {}", url, propsWithMaskedPassword(props));
            }
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
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Config: {}", propsWithMaskedPassword(config.asProperties()));
            }
            Properties props = config.asProperties();
            Field[] varsWithDefaults = combineVariables(variables,
                    JdbcConfiguration.HOSTNAME,
                    JdbcConfiguration.PORT,
                    JdbcConfiguration.USER,
                    JdbcConfiguration.PASSWORD,
                    JdbcConfiguration.DATABASE);
            String url = findAndReplace(urlPattern, props, varsWithDefaults);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Props: {}", propsWithMaskedPassword(props));
            }
            LOGGER.trace("URL: {}", url);
            Connection conn = null;
            try {
                ClassLoader driverClassLoader = classloader;
                if (driverClassLoader == null) {
                    driverClassLoader = JdbcConnection.class.getClassLoader();
                }
                Class<java.sql.Driver> driverClazz = (Class<java.sql.Driver>) Class.forName(driverClassName, true, driverClassLoader);
                java.sql.Driver driver = driverClazz.getDeclaredConstructor().newInstance();
                conn = driver.connect(url, props);
            }
            catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                throw new SQLException(e);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Connected to {} with {}", url, propsWithMaskedPassword(props));
            }
            return conn;
        };
    }

    private static Properties propsWithMaskedPassword(Properties props) {
        final Properties filtered = new Properties();
        filtered.putAll(props);
        if (props.containsKey(JdbcConfiguration.PASSWORD.name())) {
            filtered.put(JdbcConfiguration.PASSWORD.name(), "***");
        }
        return filtered;
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
            if (field != null) {
                url = findAndReplace(url, field.name(), props, field.defaultValueAsString());
            }
        }
        for (Object key : new HashSet<>(props.keySet())) {
            if (key != null) {
                url = findAndReplace(url, key.toString(), props, null);
            }
        }
        return url;
    }

    private static String findAndReplace(String url, String name, Properties props, String defaultValue) {
        if (name != null && url.contains("${" + name + "}")) {
            {
                // Otherwise, we have to remove it from the properties ...
                String value = props.getProperty(name);
                if (value != null) {
                    props.remove(name);
                }

                if (value == null) {
                    value = defaultValue;
                }

                if (value != null) {
                    // And replace the variable ...
                    url = url.replaceAll("\\$\\{" + name + "\\}", value);
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
        this(config, connectionFactory, (Operations) null);
    }

    /**
     * Create a new instance with the given configuration and connection factory.
     *
     * @param config the configuration; may not be null
     * @param connectionFactory the connection factory; may not be null
     */
    public JdbcConnection(Configuration config, ConnectionFactory connectionFactory, Supplier<ClassLoader> classLoaderSupplier) {
        this(config, connectionFactory, null, null, classLoaderSupplier);
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
        this(config, connectionFactory, initialOperations, adapter, null);
    }

    /**
     * Create a new instance with the given configuration and connection factory, and specify the operations that should be
     * run against each newly-established connection.
     *
     * @param config the configuration; may not be null
     * @param connectionFactory the connection factory; may not be null
     * @param initialOperations the initial operations that should be run on each new connection; may be null
     * @param adapter the function that can be called to update the configuration with defaults
     * @param classLoaderSupplier class loader supplier
     */
    protected JdbcConnection(Configuration config, ConnectionFactory connectionFactory, Operations initialOperations,
                             Consumer<Configuration.Builder> adapter, Supplier<ClassLoader> classLoaderSupplier) {
        this.config = adapter == null ? config : config.edit().apply(adapter).build();
        this.factory = classLoaderSupplier == null ? connectionFactory : new ConnectionFactoryDecorator(connectionFactory, classLoaderSupplier);
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

    public JdbcConnection commit() throws SQLException {
        Connection conn = connection();
        if (!conn.getAutoCommit()) {
            conn.commit();
        }
        return this;
    }

    public synchronized JdbcConnection rollback() throws SQLException {
        if (!isConnected()) {
            return this;
        }
        Connection conn = connection();
        if (!conn.getAutoCommit()) {
            conn.rollback();
        }
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
            commit();
        }
        return this;
    }

    public static interface ResultSetConsumer {
        void accept(ResultSet rs) throws SQLException;
    }

    public static interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    public static interface BlockingResultSetConsumer {
        void accept(ResultSet rs) throws SQLException, InterruptedException;
    }

    public static interface ParameterResultSetConsumer {
        void accept(List<?> parameters, ResultSet rs) throws SQLException;
    }

    public static interface MultiResultSetConsumer {
        void accept(ResultSet[] rs) throws SQLException;
    }

    public static interface BlockingMultiResultSetConsumer {
        void accept(ResultSet[] rs) throws SQLException, InterruptedException;
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
     * Execute a SQL query and map the result set into an expected type.
     * @param <T> type returned by the mapper
     *
     * @param query the SQL query
     * @param mapper the function processing the query results
     * @return the result of the mapper calculation
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public <T> T queryAndMap(String query, ResultSetMapper<T> mapper) throws SQLException {
        return queryAndMap(query, Connection::createStatement, mapper);
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

    /**
     * Execute multiple SQL prepared queries where each query is executed with the same set of parameters.
     *
     * @param multiQuery the array of prepared queries
     * @param preparer the function that supplies arguments to the prepared statement; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQuery(String[] multiQuery, StatementPreparer preparer, BlockingMultiResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        final StatementPreparer[] preparers = new StatementPreparer[multiQuery.length];
        Arrays.fill(preparers, preparer);
        return prepareQuery(multiQuery, preparers, resultConsumer);
    }

    /**
     * Execute multiple SQL prepared queries where each query is executed with the same set of parameters.
     *
     * @param multiQuery the array of prepared queries
     * @param preparers the array of functions that supply arguments to the prepared statements; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQuery(String[] multiQuery, StatementPreparer[] preparers, BlockingMultiResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        final ResultSet[] resultSets = new ResultSet[multiQuery.length];
        final PreparedStatement[] preparedStatements = new PreparedStatement[multiQuery.length];

        try {
            for (int i = 0; i < multiQuery.length; i++) {
                final String query = multiQuery[i];
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("running '{}'", query);
                }
                final PreparedStatement statement = createPreparedStatement(query);
                preparedStatements[i] = statement;
                preparers[i].accept(statement);
                resultSets[i] = statement.executeQuery();
            }
            if (resultConsumer != null) {
                resultConsumer.accept(resultSets);
            }
        }
        finally {
            for (ResultSet rs : resultSets) {
                if (rs != null) {
                    try {
                        rs.close();
                    }
                    catch (Exception ei) {
                    }
                }
            }
        }
        return this;
    }

    /**
     * Execute a SQL query and map the result set into an expected type.
     * @param <T> type returned by the mapper
     *
     * @param query the SQL query
     * @param statementFactory the function that should be used to create the statement from the connection; may not be null
     * @param mapper the function processing the query results
     * @return the result of the mapper calculation
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public <T> T queryAndMap(String query, StatementFactory statementFactory, ResultSetMapper<T> mapper) throws SQLException {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        Connection conn = connection();
        try (Statement statement = statementFactory.createStatement(conn);) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("running '{}'", query);
            }
            try (ResultSet resultSet = statement.executeQuery(query);) {
                return mapper.apply(resultSet);
            }
        }
    }

    public JdbcConnection queryWithBlockingConsumer(String query, StatementFactory statementFactory, BlockingResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
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
    public JdbcConnection prepareQueryWithBlockingConsumer(String preparedQueryString, StatementPreparer preparer, BlockingResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        final PreparedStatement statement = createPreparedStatement(preparedQueryString);
        preparer.accept(statement);
        try (ResultSet resultSet = statement.executeQuery();) {
            if (resultConsumer != null) {
                resultConsumer.accept(resultSet);
            }
        }
        return this;
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
        final PreparedStatement statement = createPreparedStatement(preparedQueryString);
        preparer.accept(statement);
        try (ResultSet resultSet = statement.executeQuery();) {
            if (resultConsumer != null) {
                resultConsumer.accept(resultSet);
            }
        }
        return this;
    }

    /**
     * Execute a SQL prepared query and map the result set into an expected type..
     * @param <T> type returned by the mapper
     *
     * @param preparedQueryString the prepared query string
     * @param preparer the function that supplied arguments to the prepared statement; may not be null
     * @param mapper the function processing the query results
     * @return the result of the mapper calculation
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public <T> T prepareQueryAndMap(String preparedQueryString, StatementPreparer preparer, ResultSetMapper<T> mapper)
            throws SQLException {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        final PreparedStatement statement = createPreparedStatement(preparedQueryString);
        preparer.accept(statement);
        try (ResultSet resultSet = statement.executeQuery();) {
            return mapper.apply(resultSet);
        }
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
        final PreparedStatement statement = createPreparedStatement(stmt);
        if (preparer != null) {
            preparer.accept(statement);
        }
        statement.execute();
        return this;
    }

    /**
     * Execute a SQL prepared query.
     *
     * @param preparedQueryString the prepared query string
     * @param parameters the list of values for parameters in the query; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQuery(String preparedQueryString, List<?> parameters,
                                       ParameterResultSetConsumer resultConsumer)
            throws SQLException {
        final PreparedStatement statement = createPreparedStatement(preparedQueryString);
        int index = 1;
        for (final Object parameter : parameters) {
            statement.setObject(index++, parameter);
        }
        try (ResultSet resultSet = statement.executeQuery()) {
            if (resultConsumer != null) {
                resultConsumer.accept(parameters, resultSet);
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
                if (i > 1) {
                    sb.append(" | ");
                }
                sb.append(Strings.setLength(rsmd.getColumnLabel(i), columnSizes[i], ' '));
            }
            lines.accept(sb.toString());
            sb.setLength(0);
            lines.accept(delimiter(columnCount, columnSizes));
            while (resultSet.next()) {
                sb.setLength(0);
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        sb.append(" | ");
                    }
                    sb.append(Strings.setLength(resultSet.getString(i), columnSizes[i], ' '));
                }
                lines.accept(sb.toString());
                sb.setLength(0);
            }
            lines.accept(delimiter(columnCount, columnSizes));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String delimiter(int columnCount, int[] columnSizes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                sb.append("---");
            }
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
                if (value != null) {
                    columnSizes[i] = Math.max(columnSizes[i], value.length());
                }
            }
        }
        resultSet.beforeFirst();
        return columnSizes;
    }

    public synchronized boolean isConnected() throws SQLException {
        if (conn == null) {
            return false;
        }
        return !conn.isClosed();
    }

    public synchronized Connection connection() throws SQLException {
        return connection(true);
    }

    public synchronized Connection connection(boolean executeOnConnect) throws SQLException {
        if (!isConnected()) {
            conn = factory.connect(JdbcConfiguration.adapt(config));
            if (!isConnected()) {
                throw new SQLException("Unable to obtain a JDBC connection");
            }
            // Always run the initial operations on this new connection
            if (initialOps != null) {
                execute(initialOps);
            }
            final String statements = config.getString(JdbcConfiguration.ON_CONNECT_STATEMENTS);
            if (statements != null && executeOnConnect) {
                final List<String> splitStatements = parseSqlStatementString(statements);
                execute(splitStatements.toArray(new String[splitStatements.size()]));
            }
        }
        return conn;
    }

    protected List<String> parseSqlStatementString(final String statements) {
        final List<String> splitStatements = new ArrayList<>();
        final char[] statementsChars = statements.toCharArray();
        StringBuilder activeStatement = new StringBuilder();
        for (int i = 0; i < statementsChars.length; i++) {
            if (statementsChars[i] == STATEMENT_DELIMITER) {
                if (i == statementsChars.length - 1) {
                    // last character so it is the delimiter
                }
                else if (statementsChars[i + 1] == STATEMENT_DELIMITER) {
                    // two semicolons in a row - escaped semicolon
                    activeStatement.append(STATEMENT_DELIMITER);
                    i++;
                }
                else {
                    // semicolon as a delimiter
                    final String trimmedStatement = activeStatement.toString().trim();
                    if (!trimmedStatement.isEmpty()) {
                        splitStatements.add(trimmedStatement);
                    }
                    activeStatement = new StringBuilder();
                }
            }
            else {
                activeStatement.append(statementsChars[i]);
            }
        }
        final String trimmedStatement = activeStatement.toString().trim();
        if (!trimmedStatement.isEmpty()) {
            splitStatements.add(trimmedStatement);
        }
        return splitStatements;
    }

    /**
     * Close the connection and release any resources.
     */
    @Override
    public synchronized void close() throws SQLException {
        if (conn != null) {
            try {
                statementCache.values().forEach(this::cleanupPreparedStatement);
                statementCache.clear();
                LOGGER.trace("Closing database connection");
                doClose();
            }
            finally {
                conn = null;
            }
        }
    }

    private void doClose() throws SQLException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        // attempting to close the connection gracefully
        Future<Object> futureClose = executor.submit(() -> {
            conn.close();
            LOGGER.info("Connection gracefully closed");
            return null;
        });
        try {
            futureClose.get(WAIT_FOR_CLOSE_SECONDS, TimeUnit.SECONDS);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new DebeziumException(e.getCause());
        }
        catch (TimeoutException | InterruptedException e) {
            LOGGER.warn("Failed to close database connection by calling close(), attempting abort()");
            conn.abort(Runnable::run);
        }
        finally {
            executor.shutdownNow();
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
                if (tableType != null) {
                    types.add(tableType);
                }
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
        if (tableNamePattern == null) {
            tableNamePattern = "%";
        }
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
    public String username() {
        return config.getString(JdbcConfiguration.USER);
    }

    /**
     * Returns the database name for this connection
     *
     * @return a {@code String}, never {@code null}
     */
    public String database() {
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
     * Resolves the supplied metadata JDBC type to a final JDBC type.
     *
     * @param metadataJdbcType the JDBC type from the underlying driver's metadata lookup
     * @param nativeType the database native type or -1 for unknown
     * @return the resolved JDBC type
     */
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        return metadataJdbcType;
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
                           TableFilter tableFilter, ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc)
            throws SQLException {
        // Before we make any changes, get the copy of the set of table IDs ...
        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        // Read the metadata for the table columns ...
        DatabaseMetaData metadata = connection().getMetaData();

        // Find regular and materialized views as they cannot be snapshotted
        final Set<TableId> viewIds = new HashSet<>();
        final Set<TableId> tableIds = new HashSet<>();

        int totalTables = 0;
        try (final ResultSet rs = metadata.getTables(databaseCatalog, schemaNamePattern, null,
                new String[]{ "VIEW", "MATERIALIZED VIEW", "TABLE" })) {
            while (rs.next()) {
                final String catalogName = rs.getString(1);
                final String schemaName = rs.getString(2);
                final String tableName = rs.getString(3);
                final String tableType = rs.getString(4);
                if ("TABLE".equals(tableType)) {
                    totalTables++;
                    TableId tableId = new TableId(catalogName, schemaName, tableName);
                    if (tableFilter == null || tableFilter.isIncluded(tableId)) {
                        tableIds.add(tableId);
                    }
                }
                else {
                    TableId tableId = new TableId(catalogName, schemaName, tableName);
                    viewIds.add(tableId);
                }
            }
        }

        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        if (totalTables == tableIds.size() || config.getBoolean(RelationalDatabaseConnectorConfig.SNAPSHOT_FULL_COLUMN_SCAN_FORCE)) {
            columnsByTable = getColumnsDetails(databaseCatalog, schemaNamePattern, null, tableFilter, columnFilter, metadata, viewIds);
        }
        else {
            for (TableId includeTable : tableIds) {
                Map<TableId, List<Column>> cols = getColumnsDetails(databaseCatalog, schemaNamePattern, includeTable.table(), tableFilter,
                        columnFilter, metadata, viewIds);
                columnsByTable.putAll(cols);
            }
        }

        // Read the metadata for the primary keys ...
        for (Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = readPrimaryKeyOrUniqueIndexNames(metadata, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            String defaultCharsetName = null; // JDBC does not expose character sets
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, defaultCharsetName);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }
    }

    private Map<TableId, List<Column>> getColumnsDetails(String databaseCatalog, String schemaNamePattern,
                                                         String tableName, TableFilter tableFilter, ColumnNameFilter columnFilter, DatabaseMetaData metadata,
                                                         final Set<TableId> viewIds)
            throws SQLException {
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        try (ResultSet columnMetadata = metadata.getColumns(databaseCatalog, schemaNamePattern, tableName, null)) {
            while (columnMetadata.next()) {
                String catalogName = columnMetadata.getString(1);
                String schemaName = columnMetadata.getString(2);
                String metaTableName = columnMetadata.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, metaTableName);

                // exclude views and non-captured tables
                if (viewIds.contains(tableId) ||
                        (tableFilter != null && !tableFilter.isIncluded(tableId))) {
                    continue;
                }

                // add all included columns
                readTableColumn(columnMetadata, tableId, columnFilter).ifPresent(column -> {
                    columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>())
                            .add(column.create());
                });
            }
        }
        return columnsByTable;
    }

    /**
     * Returns a {@link ColumnEditor} representing the current record of the given result set of column metadata, if
     * included in column.include.list.
     */
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, ColumnNameFilter columnFilter) throws SQLException {
        // Oracle drivers require this for LONG/LONGRAW to be fetched first.
        final String defaultValue = columnMetadata.getString(13);

        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            final ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }
            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));
            String autogenerated = null;
            try {
                autogenerated = columnMetadata.getString(24);
            }
            catch (SQLException e) {
                // ignore, some drivers don't have this index - e.g. Postgres
            }
            column.generated("YES".equalsIgnoreCase(autogenerated));

            column.nativeType(resolveNativeType(column.typeName()));
            column.jdbcType(resolveJdbcType(columnMetadata.getInt(5), column.nativeType()));
            if (defaultValue != null) {
                getDefaultValue(column.create(), defaultValue).ifPresent(column::defaultValue);
            }
            return Optional.of(column);
        }

        return Optional.empty();
    }

    protected Optional<Object> getDefaultValue(Column column, String defaultValue) {
        // nothing to do by default; overwrite in database specific implementation
        return Optional.empty();
    }

    public List<String> readPrimaryKeyNames(DatabaseMetaData metadata, TableId id) throws SQLException {
        final List<String> pkColumnNames = new ArrayList<>();
        try (ResultSet rs = metadata.getPrimaryKeys(id.catalog(), id.schema(), id.table())) {
            while (rs.next()) {
                String columnName = rs.getString(4);
                int columnIndex = rs.getInt(5);
                Collect.set(pkColumnNames, columnIndex - 1, columnName, null);
            }
        }
        return pkColumnNames;
    }

    public List<String> readTableUniqueIndices(DatabaseMetaData metadata, TableId id) throws SQLException {
        final List<String> uniqueIndexColumnNames = new ArrayList<>();
        try (ResultSet rs = metadata.getIndexInfo(id.catalog(), id.schema(), id.table(), true, true)) {
            String firstIndexName = null;
            while (rs.next()) {
                final String indexName = rs.getString(6);
                final String columnName = rs.getString(9);
                final int columnIndex = rs.getInt(8);
                if (firstIndexName == null) {
                    firstIndexName = indexName;
                }
                if (!isTableUniqueIndexIncluded(indexName, columnName)) {
                    continue;
                }
                // Only first unique index is taken into consideration
                if (indexName != null && !indexName.equals(firstIndexName)) {
                    return uniqueIndexColumnNames;
                }
                if (columnName != null) {
                    // The returned columnIndex is 0 when columnName is null. These are related
                    // to table statistics that get returned as part of the index descriptors
                    // and should be ignored.
                    Collect.set(uniqueIndexColumnNames, columnIndex - 1, columnName, null);
                }
            }
        }
        return uniqueIndexColumnNames;
    }

    protected List<String> readPrimaryKeyOrUniqueIndexNames(DatabaseMetaData metadata, TableId id) throws SQLException {
        final List<String> pkColumnNames = readPrimaryKeyNames(metadata, id);
        return pkColumnNames.isEmpty() ? readTableUniqueIndices(metadata, id) : pkColumnNames;
    }

    /**
     * Allows the connector implementation to determine if a table's unique index
     * should be include when resolving a table's unique indices.
     *
     * @param indexName the index name
     * @param columnName the column name
     * @return true if the index is to be included; false otherwise.
     */
    protected boolean isTableUniqueIndexIncluded(String indexName, String columnName) {
        return true;
    }

    private void cleanupPreparedStatement(PreparedStatement statement) {
        LOGGER.trace("Closing prepared statement '{}' removed from cache", statement);
        try {
            statement.close();
        }
        catch (Exception e) {
            LOGGER.info("Exception while closing a prepared statement removed from cache", e);
        }
    }

    private PreparedStatement createPreparedStatement(String preparedQueryString) {
        return statementCache.computeIfAbsent(preparedQueryString, query -> {
            try {
                LOGGER.trace("Inserting prepared statement '{}' removed from the cache", query);
                return connection().prepareStatement(query);
            }
            catch (SQLException e) {
                throw new ConnectException(e);
            }
        });
    }

    /**
     * Executes a series of statements without explicitly committing the connection.
     *
     * @param statements a series of statements to execute
     * @return this object so methods can be chained together; never null
     * @throws SQLException if anything fails
     */
    public JdbcConnection executeWithoutCommitting(String... statements) throws SQLException {
        Connection conn = connection();
        try (Statement statement = conn.createStatement()) {
            for (String stmt : statements) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Executing statement {}", stmt);
                }
                statement.execute(stmt);
            }
        }
        return this;
    }

    protected static boolean isNullable(int jdbcNullable) {
        return jdbcNullable == ResultSetMetaData.columnNullable || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
    }

    public <T> ResultSetMapper<T> singleResultMapper(ResultSetExtractor<T> extractor, String error) throws SQLException {
        return (rs) -> {
            if (rs.next()) {
                final T ret = extractor.apply(rs);
                if (!rs.next()) {
                    return ret;
                }
            }
            throw new IllegalStateException(error);
        };
    }

    public static <T> T querySingleValue(Connection connection, String queryString, StatementPreparer preparer, ResultSetExtractor<T> extractor) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString);
        preparer.accept(preparedStatement);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                final T result = extractor.apply(resultSet);
                if (!resultSet.next()) {
                    return result;
                }
            }
            throw new IllegalStateException("Exactly one result expected.");
        }
    }
}
