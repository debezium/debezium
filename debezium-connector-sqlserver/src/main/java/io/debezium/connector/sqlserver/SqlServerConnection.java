/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;

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
    private static final String ENABLE_DB_CDC;
    private static final String DISABLE_DB_CDC;
    private static final String ENABLE_TABLE_CDC;
    private static final String CDC_WRAPPERS_DML;
    private static final String GET_MAX_LSN;
    private static final String LOCK_TABLE;

    static {
        try {
            Properties statements = new Properties();
            ClassLoader classLoader = SqlServerConnection.class.getClassLoader();
            statements.load(classLoader.getResourceAsStream("statements.properties"));
            ENABLE_DB_CDC = statements.getProperty("enable_cdc_for_db");
            DISABLE_DB_CDC = statements.getProperty("disable_cdc_for_db");
            ENABLE_TABLE_CDC = statements.getProperty("enable_cdc_for_table");
            GET_MAX_LSN = statements.getProperty("get_max_lsn");
            LOCK_TABLE = statements.getProperty("lock_table");
            CDC_WRAPPERS_DML = IoUtil.read(classLoader.getResourceAsStream("generate_cdc_wrappers.sql"));
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot load SQL Server statements", e);
        }
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
        final String LSN_COUNT_ERROR = "Maximum LSN query must return exactly one value";
        return queryAndMap(GET_MAX_LSN, rs -> {
            if (rs.next()) {
                final Lsn ret = new Lsn(rs.getBytes(1));
                if (!rs.next()) {
                    return ret;
                }
            }
            throw new IllegalStateException(LSN_COUNT_ERROR);
        });
    }

    public void getChangesForTable(TableId tableId, Lsn fromLsn, Lsn toLsn, ResultSetConsumer consumer) throws SQLException {
        final String cdcNameForTable = cdcNameForTable(tableId);
        final String query = "SELECT * FROM cdc.fn_cdc_get_all_changes_" + cdcNameForTable + "(ISNULL(?,sys.fn_cdc_get_min_lsn('" + cdcNameForTable + "')), ?, N'all update old')";
        prepareQuery(query, statement -> {
            statement.setBytes(1, fromLsn.getBinary());
            statement.setBytes(2, toLsn.getBinary());
        }, consumer);
    }

    public void getChangesForTables(TableId[] tableIds, Lsn fromLsn, Lsn toLsn, MultiResultSetConsumer consumer) throws SQLException {
        final String[] queries = new String[tableIds.length];

        int idx = 0;
        for (TableId tableId: tableIds) {
            final String cdcNameForTable = cdcNameForTable(tableId);
            final String query = "SELECT * FROM cdc.fn_cdc_get_all_changes_" + cdcNameForTable + "(ISNULL(?,sys.fn_cdc_get_min_lsn('" + cdcNameForTable + "')), ?, N'all update old')";
            queries[idx++] = query;
        }
        prepareQuery(queries, statement -> {
            statement.setBytes(1, fromLsn.getBinary());
            statement.setBytes(2, toLsn.getBinary());
        }, consumer);
    }

    public Lsn incrementLsn(Lsn lsn) throws SQLException {
        final String LSN_INCREMENT_ERROR = "Increment LSN query must return exactly one value";
        final String query = "SELECT sys.fn_cdc_increment_lsn(?)";
        return prepareQueryAndMap(query, statement -> {
            statement.setBytes(1, lsn.getBinary());
        }, rs -> {
            if (rs.next()) {
                final Lsn ret = new Lsn(rs.getBytes(1));
                if (!rs.next()) {
                    return ret;
                }
            }
            throw new IllegalStateException(LSN_INCREMENT_ERROR);
        });
    }

    public Instant timestampOfLsn(Lsn lsn) throws SQLException {
        final String LSN_TIMESTAMP_ERROR = "LSN to timestamp query must return exactly one value";
        final String query = "SELECT sys.fn_cdc_map_lsn_to_time(?)";

        if (lsn.getBinary() == null) {
            return null;
        }

        return prepareQueryAndMap(query, statement -> {
            statement.setBytes(1, lsn.getBinary());
        }, rs -> {
            if (rs.next()) {
                final Timestamp ts = rs.getTimestamp(1);
                final Instant ret = ts == null ? null : ts.toInstant();
                if (!rs.next()) {
                    return ret;
                }
            }
            throw new IllegalStateException(LSN_TIMESTAMP_ERROR);
        });
    }

    public void lockTable(TableId tableId) throws SQLException {
        final String lockTableStmt = LOCK_TABLE.replace(STATEMENTS_PLACEHOLDER, tableId.table());
        execute(lockTableStmt);
    }

    private String cdcNameForTable(TableId tableId) {
        return tableId.schema() + '_' + tableId.table();
    }
}
