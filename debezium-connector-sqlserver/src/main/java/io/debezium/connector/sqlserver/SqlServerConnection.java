/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.Clock;

/**
 * {@link JdbcConnection} extension to be used with Microsoft SQL Server
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 *
 */
public class SqlServerConnection extends JdbcConnection {

    public static final String SERVER_TIMEZONE_PROP_NAME = "server.timezone";

    private static final String GET_DATABASE_NAME = "SELECT db_name()";

    private static Logger LOGGER = LoggerFactory.getLogger(SqlServerConnection.class);

    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String GET_MAX_LSN = "SELECT sys.fn_cdc_get_max_lsn()";
    private static final String LOCK_TABLE = "SELECT * FROM [#] WITH (TABLOCKX)";
    private static final String SQL_SERVER_VERSION = "SELECT @@VERSION AS 'SQL Server Version'";
    private final String lsnToTimestamp;
    private static final String INCREMENT_LSN = "SELECT sys.fn_cdc_increment_lsn(?)";
    private static final String GET_ALL_CHANGES_FOR_TABLE = "SELECT * FROM cdc.[fn_cdc_get_all_changes_#](ISNULL(?,sys.fn_cdc_get_min_lsn('#')), ?, N'all update old')";
    private static final String GET_LIST_OF_CDC_ENABLED_TABLES = "EXEC sys.sp_cdc_help_change_data_capture";
    private static final String GET_LIST_OF_NEW_CDC_ENABLED_TABLES = "SELECT * FROM cdc.change_tables WHERE start_lsn BETWEEN ? AND ?";
    private static final String GET_LIST_OF_KEY_COLUMNS = "SELECT * FROM cdc.index_columns WHERE object_id=?";

    private static final int CHANGE_TABLE_DATA_COLUMN_OFFSET = 5;

    private static final String URL_PATTERN = "jdbc:sqlserver://${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "};databaseName=${"
            + JdbcConfiguration.DATABASE + "}";
    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            SQLServerDriver.class.getName(),
            SqlServerConnection.class.getClassLoader());

    /**
     * actual name of the database, which could differ in casing from the database name given in the connector config.
     */
    private final String realDatabaseName;
    private final ZoneId transactionTimezone;
    private final SourceTimestampMode sourceTimestampMode;
    private final Clock clock;

    public static interface ResultSetExtractor<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    private final BoundedConcurrentHashMap<Lsn, Instant> lsnToInstantCache;

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config {@link Configuration} instance, may not be null.
     * @param sourceTimestampMode strategy for populating {@code source.ts_ms}.
     */
    public SqlServerConnection(Configuration config, Clock clock, SourceTimestampMode sourceTimestampMode) {
        super(config, FACTORY);
        lsnToInstantCache = new BoundedConcurrentHashMap<>(100);
        realDatabaseName = retrieveRealDatabaseName();
        boolean supportsAtTimeZone = supportsAtTimeZone();
        transactionTimezone = retrieveTransactionTimezone(supportsAtTimeZone);
        lsnToTimestamp = getLsnToTimestamp(supportsAtTimeZone);
        this.clock = clock;
        this.sourceTimestampMode = sourceTimestampMode;
    }

    /**
     * Returns the query for obtaining the LSN-to-TIMESTAMP query. On SQL Server
     * 2016 and newer, the query will normalize the value to UTC. This means that
     * the {@link #SERVER_TIMEZONE_PROP_NAME} is not necessary to be given. The
     * returned TIMESTAMP will be adjusted by the JDBC driver using this VM's TZ (as
     * required by the JDBC spec), and that same TZ will be applied when converting
     * the TIMESTAMP value into an {@code Instant}.
     */
    private static String getLsnToTimestamp(boolean supportsAtTimeZone) {
        String lsnToTimestamp = "SELECT sys.fn_cdc_map_lsn_to_time(?)";

        if (supportsAtTimeZone) {
            lsnToTimestamp = lsnToTimestamp + " AT TIME ZONE 'UTC'";
        }

        return lsnToTimestamp;
    }

    /**
     * @return the current largest log sequence number
     */
    public Lsn getMaxLsn() throws SQLException {
        return queryAndMap(GET_MAX_LSN, singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOGGER.trace("Current maximum lsn is {}", ret);
            return ret;
        }, "Maximum LSN query must return exactly one value"));
    }

    /**
     * Provides all changes recorded by the SQL Server CDC capture process for a given table.
     *
     * @param tableId - the requested table changes
     * @param fromLsn - closed lower bound of interval of changes to be provided
     * @param toLsn  - closed upper bound of interval  of changes to be provided
     * @param consumer - the change processor
     * @throws SQLException
     */
    public void getChangesForTable(TableId tableId, Lsn fromLsn, Lsn toLsn, ResultSetConsumer consumer) throws SQLException {
        final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, cdcNameForTable(tableId));
        prepareQuery(query, statement -> {
            statement.setBytes(1, fromLsn.getBinary());
            statement.setBytes(2, toLsn.getBinary());
        }, consumer);
    }

    /**
     * Provides all changes recorder by the SQL Server CDC capture process for a set of tables.
     *
     * @param changeTables - the requested tables to obtain changes for
     * @param intervalFromLsn - closed lower bound of interval of changes to be provided
     * @param intervalToLsn  - closed upper bound of interval  of changes to be provided
     * @param consumer - the change processor
     * @throws SQLException
     */
    public void getChangesForTables(SqlServerChangeTable[] changeTables, Lsn intervalFromLsn, Lsn intervalToLsn, BlockingMultiResultSetConsumer consumer)
            throws SQLException, InterruptedException {
        final String[] queries = new String[changeTables.length];
        final StatementPreparer[] preparers = new StatementPreparer[changeTables.length];

        int idx = 0;
        for (SqlServerChangeTable changeTable : changeTables) {
            final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
            queries[idx] = query;
            // If the table was added in the middle of queried buffer we need
            // to adjust from to the first LSN available
            final Lsn fromLsn = changeTable.getStartLsn().compareTo(intervalFromLsn) > 0 ? changeTable.getStartLsn() : intervalFromLsn;
            LOGGER.trace("Getting changes for table {} in range[{}, {}]", changeTable, fromLsn, intervalToLsn);
            preparers[idx] = statement -> {
                statement.setBytes(1, fromLsn.getBinary());
                statement.setBytes(2, intervalToLsn.getBinary());
            };

            idx++;
        }
        prepareQuery(queries, preparers, consumer);
    }

    /**
     * Obtain the next available position in the database log.
     *
     * @param lsn - LSN of the current position
     * @return LSN of the next position in the database
     * @throws SQLException
     */
    public Lsn incrementLsn(Lsn lsn) throws SQLException {
        final String query = INCREMENT_LSN;
        return prepareQueryAndMap(query, statement -> {
            statement.setBytes(1, lsn.getBinary());
        }, singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOGGER.trace("Increasing lsn from {} to {}", lsn, ret);
            return ret;
        }, "Increment LSN query must return exactly one value"));
    }

    /**
     * Map a commit LSN to a point in time when the commit happened.
     *
     * @param lsn - LSN of the commit
     * @return time when the commit was recorded into the database log or the
     *         current time, depending on the setting for the "source timestamp
     *         mode" option
     * @throws SQLException
     */
    public Instant timestampOfLsn(Lsn lsn) throws SQLException {
        if (SourceTimestampMode.PROCESSING.equals(sourceTimestampMode)) {
            return clock.currentTime();
        }

        if (lsn.getBinary() == null) {
            return null;
        }

        Instant cachedInstant = lsnToInstantCache.get(lsn);
        if (cachedInstant != null) {
            return cachedInstant;
        }

        return prepareQueryAndMap(lsnToTimestamp, statement -> {
            statement.setBytes(1, lsn.getBinary());
        }, singleResultMapper(rs -> {
            final Timestamp ts = rs.getTimestamp(1);
            Instant ret = (ts == null) ? null : normalize(ts);
            LOGGER.trace("Timestamp of lsn {} is {}", lsn, ret);
            if (ret != null) {
                lsnToInstantCache.put(lsn, ret);
            }
            return ret;
        }, "LSN to timestamp query must return exactly one value"));
    }

    private Instant normalize(Timestamp timestamp) {
        Instant instant = timestamp.toInstant();

        // in case the incoming timestamp was not based on UTC, shift it as per the
        // configured timezone which must match the value used by the database
        if (!transactionTimezone.getId().equals("UTC")) {
            instant = instant.atZone(transactionTimezone)
                    .toLocalDateTime()
                    .toInstant(ZoneOffset.UTC);
        }

        return instant;
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

    public Set<SqlServerChangeTable> listOfChangeTables() throws SQLException {
        final String query = GET_LIST_OF_CDC_ENABLED_TABLES;

        return queryAndMap(query, rs -> {
            final Set<SqlServerChangeTable> changeTables = new HashSet<>();
            while (rs.next()) {
                changeTables.add(
                        new SqlServerChangeTable(
                                new TableId(realDatabaseName, rs.getString(1), rs.getString(2)),
                                rs.getString(3),
                                rs.getInt(4),
                                Lsn.valueOf(rs.getBytes(6)),
                                Lsn.valueOf(rs.getBytes(7))));
            }
            return changeTables;
        });
    }

    public Set<SqlServerChangeTable> listOfNewChangeTables(Lsn fromLsn, Lsn toLsn) throws SQLException {
        final String query = GET_LIST_OF_NEW_CDC_ENABLED_TABLES;

        return prepareQueryAndMap(query,
                ps -> {
                    ps.setBytes(1, fromLsn.getBinary());
                    ps.setBytes(2, toLsn.getBinary());
                },
                rs -> {
                    final Set<SqlServerChangeTable> changeTables = new HashSet<>();
                    while (rs.next()) {
                        changeTables.add(new SqlServerChangeTable(
                                rs.getString(4),
                                rs.getInt(1),
                                Lsn.valueOf(rs.getBytes(5)),
                                Lsn.valueOf(rs.getBytes(6))));
                    }
                    return changeTables;
                });
    }

    public Table getTableSchemaFromTable(SqlServerChangeTable changeTable) throws SQLException {
        final DatabaseMetaData metadata = connection().getMetaData();

        List<Column> columns = new ArrayList<>();
        try (ResultSet rs = metadata.getColumns(
                realDatabaseName,
                changeTable.getSourceTableId().schema(),
                changeTable.getSourceTableId().table(),
                null)) {
            while (rs.next()) {
                readTableColumn(rs, changeTable.getSourceTableId(), null).ifPresent(ce -> columns.add(ce.create()));
            }
        }

        final List<String> pkColumnNames = readPrimaryKeyOrUniqueIndexNames(metadata, changeTable.getSourceTableId());
        Collections.sort(columns);
        return Table.editor()
                .tableId(changeTable.getSourceTableId())
                .addColumns(columns)
                .setPrimaryKeyNames(pkColumnNames)
                .create();
    }

    public Table getTableSchemaFromChangeTable(SqlServerChangeTable changeTable) throws SQLException {
        final DatabaseMetaData metadata = connection().getMetaData();
        final TableId changeTableId = changeTable.getChangeTableId();

        List<ColumnEditor> columnEditors = new ArrayList<>();
        try (ResultSet rs = metadata.getColumns(realDatabaseName, changeTableId.schema(), changeTableId.table(), null)) {
            while (rs.next()) {
                readTableColumn(rs, changeTableId, null).ifPresent(columnEditors::add);
            }
        }

        // The first 5 columns and the last column of the change table are CDC metadata
        final List<Column> columns = columnEditors.subList(CHANGE_TABLE_DATA_COLUMN_OFFSET, columnEditors.size() - 1).stream()
                .map(c -> c.position(c.position() - CHANGE_TABLE_DATA_COLUMN_OFFSET).create())
                .collect(Collectors.toList());

        final List<String> pkColumnNames = new ArrayList<>();
        prepareQuery(GET_LIST_OF_KEY_COLUMNS, ps -> ps.setInt(1, changeTable.getChangeTableObjectId()), rs -> {
            while (rs.next()) {
                pkColumnNames.add(rs.getString(2));
            }
        });
        Collections.sort(columns);
        return Table.editor()
                .tableId(changeTable.getSourceTableId())
                .addColumns(columns)
                .setPrimaryKeyNames(pkColumnNames)
                .create();
    }

    public synchronized void rollback() throws SQLException {
        if (isConnected()) {
            connection().rollback();
        }
    }

    public String getNameOfChangeTable(String captureName) {
        return captureName + "_CT";
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    private ZoneId retrieveTransactionTimezone(boolean supportsAtTimeZone) {
        final String serverTimezoneConfig = config().getString(SERVER_TIMEZONE_PROP_NAME);

        if (supportsAtTimeZone) {
            if (serverTimezoneConfig != null) {
                LOGGER.warn("The '{}' option should not be specified with SQL Server 2016 and newer", SERVER_TIMEZONE_PROP_NAME);
            }
        }
        else {
            if (serverTimezoneConfig == null) {
                LOGGER.warn(
                        "The '{}' option should be specified to avoid incorrect timestamp values in case of different timezones between the database server and this connector's JVM.",
                        SERVER_TIMEZONE_PROP_NAME);
            }
        }

        // Assuming UTC to be used for the ts_ms TIMESTAMP column
        // In case AT TIME ZONE is supported, UTC is what we'll request;
        // Otherwise, UTC is as good as any other guess
        return serverTimezoneConfig == null ? ZoneId.of("UTC") : ZoneId.of(serverTimezoneConfig, ZoneId.SHORT_IDS);
    }

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(
                    GET_DATABASE_NAME,
                    singleResultMapper(rs -> rs.getString(1), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain database name", e);
        }
    }

    /**
     * SELECT ... AT TIME ZONE only works on SQL Server 2016 and newer.
     */
    private boolean supportsAtTimeZone() {
        try {
            return getSqlServerVersion() > 2016;
        }
        catch (Exception e) {
            LOGGER.error("Couldn't obtain database server version; assuming 'AT TIME ZONE' is not supported.", e);
            return false;
        }
    }

    private int getSqlServerVersion() {
        try {
            // As per https://www.mssqltips.com/sqlservertip/1140/how-to-tell-what-sql-server-version-you-are-running/
            // Always beginning with 'Microsoft SQL Server NNNN'
            String version = queryAndMap(
                    SQL_SERVER_VERSION,
                    singleResultMapper(rs -> rs.getString(1), "Could not obtain SQL Server version"));

            return Integer.valueOf(version.substring(21, 25));
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't obtain database server version", e);
        }
    }
}
