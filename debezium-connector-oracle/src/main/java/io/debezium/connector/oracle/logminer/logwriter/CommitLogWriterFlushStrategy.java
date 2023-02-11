/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.logwriter;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConfiguration;

/**
 * A {@link LogWriterFlushStrategy} that uses a transaction commit to force the provided
 * connection's Oracle LogWriter (LGWR) process to flush to disk.
 *
 * @author Chris Cranford
 */
public class CommitLogWriterFlushStrategy implements LogWriterFlushStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogWriterFlushStrategy.class);

    private static final String CREATE_FLUSH_TABLE = "CREATE TABLE " + LOGMNR_FLUSH_TABLE + "(LAST_SCN NUMBER(19,0))";
    private static final String INSERT_FLUSH_TABLE = "INSERT INTO " + LOGMNR_FLUSH_TABLE + " VALUES (0)";
    private static final String UPDATE_FLUSH_TABLE = "UPDATE " + LOGMNR_FLUSH_TABLE + " SET LAST_SCN = ";
    private static final String DELETE_FLUSH_TABLE = "DELETE FROM " + LOGMNR_FLUSH_TABLE;

    private final OracleConnection connection;
    private final boolean closeConnectionOnClose;

    /**
     * Creates a transaction-commit Oracle LogWriter (LGWR) process flush strategy.
     *
     * This will use the existing database connection to make the flush and the connection will not
     * be automatically closed when the strategy is closed.
     *
     * @param connection the connection to be used to force the flush, must not be {@code null}
     */
    public CommitLogWriterFlushStrategy(OracleConnection connection) {
        this.connection = connection;
        this.closeConnectionOnClose = false;
        createFlushTableIfNotExists();
    }

    /**
     * Creates a transaction-commit Oracle LogWriter (LGWR) process flush strategy.
     *
     * This will create a new database connection based on the supplied JDBC configuration and the
     * connection will automatically be closed when the strategy is closed.
     *
     * @param jdbcConfig the jdbc configuration
     * @throws SQLException if there was a database problem
     */
    public CommitLogWriterFlushStrategy(JdbcConfiguration jdbcConfig) throws SQLException {
        this.connection = new OracleConnection(jdbcConfig);
        this.connection.setAutoCommit(false);
        this.closeConnectionOnClose = true;
        createFlushTableIfNotExists();
    }

    @Override
    public void close() {
        if (closeConnectionOnClose) {
            try {
                connection.close();
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to close connection to host '" + getHost() + "'", e);
            }
        }
    }

    @Override
    public String getHost() {
        return connection.config().getHostname();
    }

    @Override
    public void flush(Scn currentScn) {
        try {
            connection.execute(UPDATE_FLUSH_TABLE + currentScn);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to flush Oracle LogWriter (LGWR) buffers to disk", e);
        }
    }

    /**
     * Makes sure that the flush table is created in the database and that it at least has 1 row of data
     * so that when flushes occur that the update succeeds without failure.
     */
    private void createFlushTableIfNotExists() {
        try {
            if (!connection.isTableExists(LOGMNR_FLUSH_TABLE)) {
                connection.executeWithoutCommitting(CREATE_FLUSH_TABLE);
            }

            fixMultiRowDataBug();

            if (connection.isTableEmpty(LOGMNR_FLUSH_TABLE)) {
                connection.executeWithoutCommitting(INSERT_FLUSH_TABLE);
                connection.commit();
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to create flush table", e);
        }
    }

    /**
     * Cleans and resets the state of the flush table if multiple rows are detected.
     *
     * This bug was introduced in Debezium 1.7.0.Final by mistake and this function will self-correct
     * the data managed by the table.
     *
     * @throws SQLException if a database exception occurs
     */
    private void fixMultiRowDataBug() throws SQLException {
        if (connection.getRowCount(LOGMNR_FLUSH_TABLE) > 1L) {
            LOGGER.warn("DBZ-4118: The flush table, {}, has multiple rows and has been corrected.", LOGMNR_FLUSH_TABLE);
            connection.executeWithoutCommitting(DELETE_FLUSH_TABLE);
            connection.executeWithoutCommitting(INSERT_FLUSH_TABLE);
            connection.commit();
        }
    }
}
