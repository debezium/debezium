/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.util.IoUtil;

/**
 * {@link JdbcConnection} extension to be used with Microsoft SQL Server
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 *
 */
public class SqlServerConnection extends JdbcConnection {

    private static Logger LOGGER = LoggerFactory.getLogger(SqlServerConnection.class);

    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String ENABLE_DB_CDC = "IF EXISTS(select 1 from sys.databases where name='#' AND is_cdc_enabled=0)\n"
            + "EXEC sys.sp_cdc_enable_db";
    private static final String DISABLE_DB_CDC = "IF EXISTS(select 1 from sys.databases where name='#' AND is_cdc_enabled=1)\n"
            + "EXEC sys.sp_cdc_disable_db";
    private static final String ENABLE_TABLE_CDC = "IF EXISTS(select 1 from sys.tables where name = '#' AND is_tracked_by_cdc=0)\n"
            + "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'#', @role_name = NULL, @supports_net_changes = 0";
    private static final String CDC_WRAPPERS_DML;
    private static final String GET_MAX_LSN = "SELECT sys.fn_cdc_get_max_lsn()";
    private static final String LOCK_TABLE = "SELECT * FROM # WITH (TABLOCKX)";
    private static final String LSN_TO_TIMESTAMP = "SELECT sys.fn_cdc_map_lsn_to_time(?)";
    private static final String INCREMENT_LSN = "SELECT sys.fn_cdc_increment_lsn(?)";
    private static final String GET_ALL_CHANGES_FOR_TABLE = "SELECT * FROM cdc.fn_cdc_get_all_changes_#(ISNULL(?,sys.fn_cdc_get_min_lsn('#')), ?, N'all update old')";

    static {
        try {
            ClassLoader classLoader = SqlServerConnection.class.getClassLoader();
            CDC_WRAPPERS_DML = IoUtil.read(classLoader.getResourceAsStream("generate_cdc_wrappers.sql"));
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot load SQL Server statements", e);
        }
    }

    private static interface ResultSetExtractor<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config
     *            {@link Configuration} instance, may not be null.
     * @param factory a factory building the connection string
     */
    public SqlServerConnection(Configuration config, ConnectionFactory factory) {
        super(config, factory);
    }

    /**
     * Enables CDC for a given database, if not already enabled.
     *
     * @param name
     *            the name of the DB, may not be {@code null}
     * @throws SQLException
     *             if anything unexpected fails
     */
    public void enableDbCdc(String name) throws SQLException {
        Objects.requireNonNull(name);
        execute(ENABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, name));
    }

    /**
     * Disables CDC for a given database, if not already disabled.
     *
     * @param name
     *            the name of the DB, may not be {@code null}
     * @throws SQLException
     *             if anything unexpected fails
     */
    public void disableDbCdc(String name) throws SQLException {
        Objects.requireNonNull(name);
        execute(DISABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, name));
    }

    /**
     * Enables CDC for a table if not already enabled and generates the wrapper
     * functions for that table.
     *
     * @param name
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public void enableTableCdc(String name) throws SQLException {
        Objects.requireNonNull(name);
        String enableCdcForTableStmt = ENABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, name);
        String generateWrapperFunctionsStmts = CDC_WRAPPERS_DML.replaceAll(STATEMENTS_PLACEHOLDER, name);
        execute(enableCdcForTableStmt, generateWrapperFunctionsStmts);
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
     * @param tableIds - the requested tables to obtain changes for
     * @param fromLsn - closed lower bound of interval of changes to be provided
     * @param toLsn  - closed upper bound of interval  of changes to be provided
     * @param consumer - the change processor
     * @throws SQLException
     */
    public void getChangesForTables(TableId[] tableIds, Lsn fromLsn, Lsn toLsn, BlockingMultiResultSetConsumer consumer) throws SQLException, InterruptedException {
        final String[] queries = new String[tableIds.length];

        int idx = 0;
        for (TableId tableId: tableIds) {
            final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, cdcNameForTable(tableId));
            queries[idx++] = query;
            LOGGER.trace("Getting changes for table {} in range[{}, {}]", tableId, fromLsn, toLsn);
        }
        prepareQuery(queries, statement -> {
            statement.setBytes(1, fromLsn.getBinary());
            statement.setBytes(2, toLsn.getBinary());
        }, consumer);
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
     * @return time when the commit was recorded into the database log
     * @throws SQLException
     */
    public Instant timestampOfLsn(Lsn lsn) throws SQLException {
        final String query = LSN_TO_TIMESTAMP;

        if (lsn.getBinary() == null) {
            return null;
        }
        return prepareQueryAndMap(query, statement -> {
            statement.setBytes(1, lsn.getBinary());
        }, singleResultMapper(rs -> {
            final Timestamp ts = rs.getTimestamp(1);
            final Instant ret = (ts == null) ? null : ts.toInstant();
            LOGGER.trace("Timestamp of lsn {} is {}", lsn, ret);
            return ret;
        }, "LSN to timestamp query must return exactly one value"));
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

    private <T> ResultSetMapper<T> singleResultMapper(ResultSetExtractor<T> extractor, String error) throws SQLException {
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
}
