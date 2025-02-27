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
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

/**
 * A {@link LogWriterFlushStrategy} that uses a transaction commit to force the provided
 * connection's Oracle LogWriter (LGWR) process to flush to disk.
 *
 * @author Chris Cranford
 */
public class CommitLogWriterFlushStrategy implements LogWriterFlushStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogWriterFlushStrategy.class);

    private static final String CREATE_FLUSH_TABLE = "CREATE TABLE %s (LAST_SCN NUMBER(19,0))";
    private static final String INSERT_FLUSH_TABLE = "INSERT INTO %s VALUES (0)";
    private static final String UPDATE_FLUSH_TABLE = "UPDATE %s SET LAST_SCN = ";
    private static final String DELETE_FLUSH_TABLE = "DELETE FROM %s";

    private final String flushTableName;
    private final TableId flushTableId;
    private final String databasePdbName;
    private final OracleConnection connection;

    /**
     * Creates a transaction-commit Oracle LogWriter (LGWR) process flush strategy.
     *
     * This will use the existing database connection to make the flush and the connection will not
     * be automatically closed when the strategy is closed.
     *
     * @param connectorConfig the connector configuration, must not be {@code null}
     * @param connection the connection to be used to force the flush, must not be {@code null}
     */
    public CommitLogWriterFlushStrategy(OracleConnectorConfig connectorConfig, OracleConnection connection) {
        this.flushTableId = TableId.parse(connectorConfig.getLogMiningFlushTableName());
        this.flushTableName = flushTableId.toDoubleQuotedString();
        this.databasePdbName = connectorConfig.getPdbName();
        this.connection = connection;
        createFlushTableIfNotExists();
    }

    @Override
    public void close() {
    }

    @Override
    public String getHost() {
        return connection.config().getHostname();
    }

    @Override
    public void flush(Scn currentScn) {
        try {
            if (!Strings.isNullOrEmpty(databasePdbName)) {
                connection.setSessionToPdb(databasePdbName);
            }
            connection.execute(String.format(UPDATE_FLUSH_TABLE, flushTableName) + currentScn);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to flush Oracle LogWriter (LGWR) buffers to disk", e);
        }
        finally {
            if (!Strings.isNullOrEmpty(databasePdbName)) {
                connection.resetSessionToCdb();
            }
        }
    }

    /**
     * Makes sure that the flush table is created in the database and that it at least has 1 row of data
     * so that when flushes occur that the update succeeds without failure.
     */
    private void createFlushTableIfNotExists() {
        try {
            if (!Strings.isNullOrBlank(databasePdbName)) {
                connection.setSessionToPdb(databasePdbName);
            }

            if (!connection.isTableExists(flushTableId)) {
                connection.executeWithoutCommitting(String.format(CREATE_FLUSH_TABLE, flushTableName));
            }

            fixMultiRowDataBug();

            if (connection.isTableEmpty(flushTableId)) {
                connection.executeWithoutCommitting(String.format(INSERT_FLUSH_TABLE, flushTableName));
                connection.commit();
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to create flush table", e);
        }
        finally {
            if (!Strings.isNullOrEmpty(databasePdbName)) {
                connection.resetSessionToCdb();
            }
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
        if (connection.getRowCount(flushTableId) > 1L) {
            LOGGER.warn("DBZ-4118: The flush table, {}, has multiple rows and has been corrected.", flushTableId);
            connection.executeWithoutCommitting(String.format(DELETE_FLUSH_TABLE, flushTableName));
            connection.executeWithoutCommitting(String.format(INSERT_FLUSH_TABLE, flushTableName));
            connection.commit();
        }
    }
}
