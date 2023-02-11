/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * {@link JdbcConnection} extension to be used with Microsoft SQL Server
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 *
 */
public class SqlServerConnection extends JdbcConnection {

    public static final String INSTANCE_NAME = "instance";

    private static final String GET_DATABASE_NAME = "SELECT name FROM sys.databases WHERE name = ?";

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnection.class);

    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String DATABASE_NAME_PLACEHOLDER = "#db";
    private static final String GET_MAX_LSN = "SELECT [#db].sys.fn_cdc_get_max_lsn()";
    private static final String GET_MAX_TRANSACTION_LSN = "SELECT MAX(start_lsn) FROM [#db].cdc.lsn_time_mapping WHERE tran_id <> 0x00";
    private static final String GET_NTH_TRANSACTION_LSN_FROM_BEGINNING = "SELECT MAX(start_lsn) FROM (SELECT TOP (?) start_lsn FROM [#db].cdc.lsn_time_mapping WHERE tran_id <> 0x00 ORDER BY start_lsn) as next_lsns";
    private static final String GET_NTH_TRANSACTION_LSN_FROM_LAST = "SELECT MAX(start_lsn) FROM (SELECT TOP (? + 1) start_lsn FROM [#db].cdc.lsn_time_mapping WHERE start_lsn >= ? AND tran_id <> 0x00 ORDER BY start_lsn) as next_lsns";

    private static final String GET_MIN_LSN = "SELECT [#db].sys.fn_cdc_get_min_lsn('#')";
    private static final String LOCK_TABLE = "SELECT * FROM [#] WITH (TABLOCKX)";
    private static final String INCREMENT_LSN = "SELECT [#db].sys.fn_cdc_increment_lsn(?)";
    private static final String GET_ALL_CHANGES_FOR_TABLE = "SELECT *# FROM [#db].cdc.[fn_cdc_get_all_changes_#](?, ?, N'all update old') order by [__$start_lsn] ASC, [__$seqval] ASC, [__$operation] ASC";
    private final String get_all_changes_for_table;
    protected static final String LSN_TIMESTAMP_SELECT_STATEMENT = "TODATETIMEOFFSET([#db].sys.fn_cdc_map_lsn_to_time([__$start_lsn]), DATEPART(TZOFFSET, SYSDATETIMEOFFSET()))";

    /**
     * Queries the list of captured column names and their change table identifiers in the given database.
     */
    private static final String GET_CAPTURED_COLUMNS = "SELECT object_id, column_name" +
            " FROM [#db].cdc.captured_columns" +
            " ORDER BY object_id, column_id";

    /**
     * Queries the list of capture instances in the given database.
     *
     * If two or more capture instances with the same start LSN are available for a given source table,
     * only the newest one will be returned.
     *
     * We use a query instead of {@code sys.sp_cdc_help_change_data_capture} because:
     *   1. The stored procedure doesn't allow filtering capture instances by start LSN.
     *   2. There is no way to use the result returned by a stored procedure in a query.
     */
    private static final String GET_CHANGE_TABLES = "WITH ordered_change_tables" +
            " AS (SELECT ROW_NUMBER() OVER (PARTITION BY ct.source_object_id, ct.start_lsn ORDER BY ct.create_date DESC) AS ct_sequence," +
            " ct.*" +
            " FROM [#db].cdc.change_tables AS ct#)" +
            " SELECT OBJECT_SCHEMA_NAME(source_object_id, DB_ID(?))," +
            " OBJECT_NAME(source_object_id, DB_ID(?))," +
            " capture_instance," +
            " object_id," +
            " start_lsn" +
            " FROM ordered_change_tables WHERE ct_sequence = 1";

    private static final String GET_NEW_CHANGE_TABLES = "SELECT * FROM [#db].cdc.change_tables WHERE start_lsn BETWEEN ? AND ?";
    private static final String OPENING_QUOTING_CHARACTER = "[";
    private static final String CLOSING_QUOTING_CHARACTER = "]";

    private static final String URL_PATTERN = "jdbc:sqlserver://${" + JdbcConfiguration.HOSTNAME + "}";

    private final SqlServerJdbcConfiguration config;
    private final boolean useSingleDatabase;
    private final String getAllChangesForTable;
    private final int queryFetchSize;

    private final SqlServerDefaultValueConverter defaultValueConverter;

    private boolean optionRecompile;

    private static final Field AGENT_STATUS_QUERY = Field.create("sqlserver.agent.status.query")
            .withDescription("Query to get the running status of the SQL Server Agent")
            .withDefault(
                    "SELECT CASE WHEN dss.[status]=4 THEN 1 ELSE 0 END AS isRunning FROM [#db].sys.dm_server_services dss WHERE dss.[servicename] LIKE N'SQL Server Agent (%';");

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config              {@link Configuration} instance, may not be null.
     * @param valueConverters     {@link SqlServerValueConverters} instance
     * @param skippedOperations   a set of {@link Envelope.Operation} to skip in streaming
     */
    public SqlServerConnection(SqlServerJdbcConfiguration config, SqlServerValueConverters valueConverters,
                               Set<Envelope.Operation> skippedOperations,
                               boolean useSingleDatabase) {
        super(config, createConnectionFactory(config, useSingleDatabase), OPENING_QUOTING_CHARACTER, CLOSING_QUOTING_CHARACTER);

        defaultValueConverter = new SqlServerDefaultValueConverter(this::connection, valueConverters);
        this.queryFetchSize = config().getInteger(CommonConnectorConfig.QUERY_FETCH_SIZE);

        if (hasSkippedOperations(skippedOperations)) {
            Set<String> skippedOps = new HashSet<>();
            StringBuilder getAllChangesForTableStatement = new StringBuilder(
                    "SELECT *# FROM [#db].cdc.[fn_cdc_get_all_changes_#](?, ?, N'all update old') WHERE __$operation NOT IN (");
            skippedOperations.forEach((Envelope.Operation operation) -> {
                // This number are the __$operation number in the SQLServer
                // https://docs.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql?view=sql-server-ver15#table-returned
                switch (operation) {
                    case CREATE:
                        skippedOps.add("2");
                        break;
                    case UPDATE:
                        skippedOps.add("3");
                        skippedOps.add("4");
                        break;
                    case DELETE:
                        skippedOps.add("1");
                        break;
                }
            });
            getAllChangesForTableStatement.append(String.join(",", skippedOps));
            getAllChangesForTableStatement.append(") order by [__$start_lsn] ASC, [__$seqval] ASC, [__$operation] ASC");
            get_all_changes_for_table = getAllChangesForTableStatement.toString();
        }
        else {
            get_all_changes_for_table = GET_ALL_CHANGES_FOR_TABLE;

        }

        getAllChangesForTable = get_all_changes_for_table.replaceFirst(STATEMENTS_PLACEHOLDER,
                Matcher.quoteReplacement(", " + LSN_TIMESTAMP_SELECT_STATEMENT));
        this.config = config;
        this.useSingleDatabase = useSingleDatabase;

        this.optionRecompile = false;
    }

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config              {@link Configuration} instance, may not be null.
     * @param valueConverters     {@link SqlServerValueConverters} instance
     * @param skippedOperations   a set of {@link Envelope.Operation} to skip in streaming
     * @param optionRecompile     Includes query option RECOMPILE on incremental snapshots
     */
    public SqlServerConnection(SqlServerJdbcConfiguration config, SqlServerValueConverters valueConverters,
                               Set<Envelope.Operation> skippedOperations, boolean useSingleDatabase,
                               boolean optionRecompile) {
        this(config, valueConverters, skippedOperations, useSingleDatabase);

        this.optionRecompile = optionRecompile;
    }

    private boolean hasSkippedOperations(Set<Envelope.Operation> skippedOperations) {
        if (!skippedOperations.isEmpty()) {
            for (Envelope.Operation operation : skippedOperations) {
                switch (operation) {
                    case CREATE:
                    case UPDATE:
                    case DELETE:
                        return true;
                }
            }
        }
        return false;
    }

    private static ConnectionFactory createConnectionFactory(SqlServerJdbcConfiguration config, boolean useSingleDatabase) {
        return JdbcConnection.patternBasedFactory(createUrlPattern(config, useSingleDatabase),
                SQLServerDriver.class.getName(),
                SqlServerConnection.class.getClassLoader(),
                JdbcConfiguration.PORT.withDefault(SqlServerConnectorConfig.PORT.defaultValueAsString()));
    }

    @VisibleForTesting
    protected static String createUrlPattern(SqlServerJdbcConfiguration config, boolean useSingleDatabase) {
        String pattern = URL_PATTERN;
        if (config.getInstance() != null) {
            pattern += "\\" + config.getInstance();
            if (config.getPortAsString() != null) {
                pattern += ":${" + JdbcConfiguration.PORT + "}";
            }
        }
        else {
            pattern += ":${" + JdbcConfiguration.PORT + "}";
        }
        if (useSingleDatabase) {
            pattern += ";databaseName=${" + JdbcConfiguration.DATABASE + "}";
        }

        return pattern;
    }

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString() {
        return connectionString(createUrlPattern(config, useSingleDatabase));
    }

    @Override
    public synchronized Connection connection(boolean executeOnConnect) throws SQLException {
        boolean connected = isConnected();
        Connection connection = super.connection(executeOnConnect);

        if (!connected) {
            connection.setAutoCommit(false);
        }

        return connection;
    }

    /**
     * @return the current largest log sequence number
     */
    public Lsn getMaxLsn(String databaseName) throws SQLException {
        return queryAndMap(replaceDatabaseNamePlaceholder(GET_MAX_LSN, databaseName), singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOGGER.trace("Current maximum lsn is {}", ret);
            return ret;
        }, "Maximum LSN query must return exactly one value"));
    }

    /**
     * @return the log sequence number of the most recent transaction
     *         that isn't further than {@code maxOffset} from the beginning.
     */
    public Lsn getNthTransactionLsnFromBeginning(String databaseName, int maxOffset) throws SQLException {
        return prepareQueryAndMap(replaceDatabaseNamePlaceholder(GET_NTH_TRANSACTION_LSN_FROM_BEGINNING, databaseName), statement -> {
            statement.setInt(1, maxOffset);
        }, singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOGGER.trace("Nth lsn from beginning is {}", ret);
            return ret;
        }, "Nth LSN query must return exactly one value"));
    }

    /**
     * @return the log sequence number of the most recent transaction
     *         that isn't further than {@code maxOffset} from {@code lastLsn}.
     */
    public Lsn getNthTransactionLsnFromLast(String databaseName, Lsn lastLsn, int maxOffset) throws SQLException {
        return prepareQueryAndMap(replaceDatabaseNamePlaceholder(GET_NTH_TRANSACTION_LSN_FROM_LAST, databaseName), statement -> {
            statement.setInt(1, maxOffset);
            statement.setBytes(2, lastLsn.getBinary());
        }, singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOGGER.trace("Nth lsn from last is {}", ret);
            return ret;
        }, "Nth LSN query must return exactly one value"));
    }

    /**
     * @return the log sequence number of the most recent transaction.
     */
    public Lsn getMaxTransactionLsn(String databaseName) throws SQLException {
        return queryAndMap(replaceDatabaseNamePlaceholder(GET_MAX_TRANSACTION_LSN, databaseName), singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOGGER.trace("Max transaction lsn is {}", ret);
            return ret;
        }, "Max transaction LSN query must return exactly one value"));
    }

    /**
     * @return the smallest log sequence number of table
     */
    public Lsn getMinLsn(String databaseName, String changeTableName) throws SQLException {
        String query = replaceDatabaseNamePlaceholder(GET_MIN_LSN, databaseName)
                .replace(STATEMENTS_PLACEHOLDER, changeTableName);
        return queryAndMap(query, singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOGGER.trace("Current minimum lsn is {}", ret);
            return ret;
        }, "Minimum LSN query must return exactly one value"));
    }

    /**
     * Provides all changes recorder by the SQL Server CDC capture process for a set of tables.
     *
     * @param databaseName - the name of the database to query
     * @param changeTables - the requested tables to obtain changes for
     * @param intervalFromLsn - closed lower bound of interval of changes to be provided
     * @param intervalToLsn  - closed upper bound of interval  of changes to be provided
     * @param consumer - the change processor
     * @throws SQLException
     */
    public void getChangesForTables(String databaseName, SqlServerChangeTable[] changeTables, Lsn intervalFromLsn,
                                    Lsn intervalToLsn, BlockingMultiResultSetConsumer consumer)
            throws SQLException, InterruptedException {
        final String[] queries = new String[changeTables.length];
        final StatementPreparer[] preparers = new StatementPreparer[changeTables.length];

        int idx = 0;
        for (SqlServerChangeTable changeTable : changeTables) {
            final String query = replaceDatabaseNamePlaceholder(getAllChangesForTable, databaseName)
                    .replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
            queries[idx] = query;
            // If the table was added in the middle of queried buffer we need
            // to adjust from to the first LSN available
            final Lsn fromLsn = getFromLsn(databaseName, changeTable, intervalFromLsn);
            LOGGER.trace("Getting changes for table {} in range[{}, {}]", changeTable, fromLsn, intervalToLsn);
            preparers[idx] = statement -> {
                if (queryFetchSize > 0) {
                    statement.setFetchSize(queryFetchSize);
                }
                statement.setBytes(1, fromLsn.getBinary());
                statement.setBytes(2, intervalToLsn.getBinary());
            };

            idx++;
        }
        prepareQuery(queries, preparers, consumer);
    }

    private Lsn getFromLsn(String databaseName, SqlServerChangeTable changeTable, Lsn intervalFromLsn) throws SQLException {
        Lsn fromLsn = changeTable.getStartLsn().compareTo(intervalFromLsn) > 0 ? changeTable.getStartLsn() : intervalFromLsn;
        return fromLsn.getBinary() != null ? fromLsn : getMinLsn(databaseName, changeTable.getCaptureInstance());
    }

    /**
     * Obtain the next available position in the database log.
     *
     * @param databaseName - the name of the database that the LSN belongs to
     * @param lsn - LSN of the current position
     * @return LSN of the next position in the database
     * @throws SQLException
     */
    public Lsn incrementLsn(String databaseName, Lsn lsn) throws SQLException {
        return prepareQueryAndMap(replaceDatabaseNamePlaceholder(INCREMENT_LSN, databaseName), statement -> {
            statement.setBytes(1, lsn.getBinary());
        }, singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOGGER.trace("Increasing lsn from {} to {}", lsn, ret);
            return ret;
        }, "Increment LSN query must return exactly one value"));
    }

    /**
     * Check if the user with which connection object is created has
     * access to CDC table.
     *
     * @return boolean indicating the presence/absence of access
     */
    public boolean checkIfConnectedUserHasAccessToCDCTable(String databaseName) throws SQLException {
        final AtomicBoolean userHasAccess = new AtomicBoolean();
        final String query = replaceDatabaseNamePlaceholder("EXEC [#db].sys.sp_cdc_help_change_data_capture", databaseName);
        this.query(query, rs -> userHasAccess.set(rs.next()));
        return userHasAccess.get();
    }

    /**
     * Creates an exclusive lock for a given table.
     *
     * @param tableId to be locked
     * @throws SQLException
     */
    public void lockTable(TableId tableId) throws SQLException {
        final String lockTableStmt = LOCK_TABLE.replace(STATEMENTS_PLACEHOLDER, tableId.table());
        execute(lockTableStmt);
    }

    private String cdcNameForTable(TableId tableId) {
        return tableId.schema() + '_' + tableId.table();
    }

    public static class CdcEnabledTable {
        private final String tableId;
        private final String captureName;
        private final Lsn fromLsn;

        private CdcEnabledTable(String tableId, String captureName, Lsn fromLsn) {
            this.tableId = tableId;
            this.captureName = captureName;
            this.fromLsn = fromLsn;
        }

        public String getTableId() {
            return tableId;
        }

        public String getCaptureName() {
            return captureName;
        }

        public Lsn getFromLsn() {
            return fromLsn;
        }
    }

    public List<SqlServerChangeTable> getChangeTables(String databaseName) throws SQLException {
        return getChangeTables(databaseName, Lsn.NULL);
    }

    public List<SqlServerChangeTable> getChangeTables(String databaseName, Lsn toLsn) throws SQLException {
        Map<Integer, List<String>> columns = queryAndMap(
                replaceDatabaseNamePlaceholder(GET_CAPTURED_COLUMNS, databaseName),
                rs -> {
                    Map<Integer, List<String>> result = new HashMap<>();
                    while (rs.next()) {
                        int changeTableObjectId = rs.getInt(1);
                        if (!result.containsKey(changeTableObjectId)) {
                            result.put(changeTableObjectId, new LinkedList<>());
                        }

                        result.get(changeTableObjectId).add(rs.getString(2));
                    }
                    return result;
                });
        final ResultSetMapper<List<SqlServerChangeTable>> mapper = rs -> {
            final List<SqlServerChangeTable> changeTables = new ArrayList<>();
            while (rs.next()) {
                int changeTableObjectId = rs.getInt(4);
                changeTables.add(
                        new SqlServerChangeTable(
                                new TableId(databaseName, rs.getString(1), rs.getString(2)),
                                rs.getString(3),
                                changeTableObjectId,
                                Lsn.valueOf(rs.getBytes(5)),
                                columns.get(changeTableObjectId)));
            }
            return changeTables;
        };

        String query = replaceDatabaseNamePlaceholder(GET_CHANGE_TABLES, databaseName);

        if (toLsn.isAvailable()) {
            return prepareQueryAndMap(query.replace(STATEMENTS_PLACEHOLDER, " WHERE ct.start_lsn <= ?"),
                    ps -> {
                        ps.setBytes(1, toLsn.getBinary());
                        ps.setString(2, databaseName);
                        ps.setString(3, databaseName);
                    },
                    mapper);
        }
        else {
            return prepareQueryAndMap(query.replace(STATEMENTS_PLACEHOLDER, ""),
                    ps -> {
                        ps.setString(1, databaseName);
                        ps.setString(2, databaseName);
                    },
                    mapper);
        }
    }

    public List<SqlServerChangeTable> getNewChangeTables(String databaseName, Lsn fromLsn, Lsn toLsn) throws SQLException {
        final String query = replaceDatabaseNamePlaceholder(GET_NEW_CHANGE_TABLES, databaseName);

        return prepareQueryAndMap(query,
                ps -> {
                    ps.setBytes(1, fromLsn.getBinary());
                    ps.setBytes(2, toLsn.getBinary());
                },
                rs -> {
                    final List<SqlServerChangeTable> changeTables = new ArrayList<>();
                    while (rs.next()) {
                        changeTables.add(new SqlServerChangeTable(
                                rs.getString(4),
                                rs.getInt(1),
                                Lsn.valueOf(rs.getBytes(5))));
                    }
                    return changeTables;
                });
    }

    public Table getTableSchemaFromTable(String databaseName, SqlServerChangeTable changeTable) throws SQLException {
        final DatabaseMetaData metadata = connection().getMetaData();

        List<Column> columns = new ArrayList<>();
        try (ResultSet rs = metadata.getColumns(
                databaseName,
                changeTable.getSourceTableId().schema(),
                changeTable.getSourceTableId().table(),
                null)) {
            while (rs.next()) {
                readTableColumn(rs, changeTable.getSourceTableId(), null).ifPresent(ce -> {
                    // Filter out columns not included in the change table.
                    if (changeTable.getCapturedColumns().contains(ce.name())) {
                        columns.add(ce.create());
                    }
                });
            }
        }

        final List<String> pkColumnNames = readPrimaryKeyOrUniqueIndexNames(metadata, changeTable.getSourceTableId()).stream()
                .filter(column -> changeTable.getCapturedColumns().contains(column))
                .collect(Collectors.toList());
        Collections.sort(columns);
        return Table.editor()
                .tableId(changeTable.getSourceTableId())
                .addColumns(columns)
                .setPrimaryKeyNames(pkColumnNames)
                .create();
    }

    public String getNameOfChangeTable(String captureName) {
        return captureName + "_CT";
    }

    /**
     * Retrieve the name of the database in the original case as it's defined on the server.
     *
     * Although SQL Server supports case-insensitive collations, the connector uses the database name to build the
     * produced records' source info and, subsequently, the keys of its committed offset messages. This value
     * must remain the same during the lifetime of the connector regardless of the case used in the connector
     * configuration.
     */
    public String retrieveRealDatabaseName(String databaseName) {
        try {
            return prepareQueryAndMap(GET_DATABASE_NAME,
                    ps -> ps.setString(1, databaseName),
                    singleResultMapper(rs -> rs.getString(1), "Could not retrieve exactly one database name"));
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain database name", e);
        }
    }

    @Override
    protected boolean isTableUniqueIndexIncluded(String indexName, String columnName) {
        // SQL Server provides indices also without index name
        // so we need to ignore them
        return indexName != null;
    }

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        final ResultSetMetaData metaData = rs.getMetaData();
        final int columnType = metaData.getColumnType(columnIndex);

        if (columnType == Types.TIME) {
            return rs.getTimestamp(columnIndex);
        }
        else {
            return super.getColumnValue(rs, columnIndex, column, table);
        }
    }

    @Override
    public String buildSelectWithRowLimits(TableId tableId, int limit, String projection, Optional<String> condition,
                                           Optional<String> additionalCondition, String orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT TOP ");
        sql
                .append(limit)
                .append(' ')
                .append(projection)
                .append(" FROM ");
        sql.append(quotedTableIdString(tableId));
        if (condition.isPresent()) {
            sql
                    .append(" WHERE ")
                    .append(condition.get());
            if (additionalCondition.isPresent()) {
                sql.append(" AND ");
                sql.append(additionalCondition.get());
            }
        }
        else if (additionalCondition.isPresent()) {
            sql.append(" WHERE ");
            sql.append(additionalCondition.get());
        }
        sql
                .append(" ORDER BY ")
                .append(orderBy);
        if (this.optionRecompile) {
            sql
                    .append(" OPTION(RECOMPILE)");
        }
        return sql.toString();
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        return "[" + tableId.catalog() + "].[" + tableId.schema() + "].[" + tableId.table() + "]";
    }

    private String replaceDatabaseNamePlaceholder(String sql, String databaseName) {
        return sql.replace(DATABASE_NAME_PLACEHOLDER, databaseName);
    }

    public SqlServerDefaultValueConverter getDefaultValueConverter() {
        return defaultValueConverter;
    }

    public boolean isAgentRunning(String databaseName) throws SQLException {
        final String query = replaceDatabaseNamePlaceholder(config().getString(AGENT_STATUS_QUERY), databaseName);
        final boolean isRunning = queryAndMap(query,
                singleResultMapper(rs -> rs.getBoolean(1), "SQL Server Agent running status query must return exactly one value"));
        return isRunning;
    }
}
