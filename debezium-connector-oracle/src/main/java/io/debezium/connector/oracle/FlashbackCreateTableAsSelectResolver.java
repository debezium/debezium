/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection.NonRelationalTableException;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactory;
import io.debezium.relational.TableId;

/**
 * Resolves the definition of a table created by {@code CREATE TABLE AS SELECT} by reading the data dictionary
 * as it was at the statement's commit: the statement does not carry the columns, and the table is first
 * visible at its transaction's commit. The read needs the {@code EXECUTE} privilege on {@code DBMS_FLASHBACK};
 * when it fails, the event processing failure handling mode decides whether the connector stops or the schema
 * change is skipped.
 *
 * @author Chris Cranford
 */
public class FlashbackCreateTableAsSelectResolver implements CreateTableAsSelectResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlashbackCreateTableAsSelectResolver.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnectionFactory connectionFactory;
    private final AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics;
    private final Scn commitScn;

    /**
     * @param connectorConfig the connector configuration, should not be {@code null}
     * @param connectionFactory the connection factory whose connections reach the primary database, should not be {@code null}
     * @param streamingMetrics the streaming metrics, should not be {@code null}
     * @param commitScn the system change number at which the statement's transaction committed, should not be {@code null}
     */
    public FlashbackCreateTableAsSelectResolver(OracleConnectorConfig connectorConfig, OracleConnectionFactory connectionFactory,
                                                AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics, Scn commitScn) {
        this.connectorConfig = connectorConfig;
        this.connectionFactory = connectionFactory;
        this.streamingMetrics = streamingMetrics;
        this.commitScn = commitScn;
    }

    @Override
    public String resolveCreateTableDdl(TableId tableId) {
        try {
            return readTableDdl(tableId, commitScn);
        }
        catch (SQLException | NonRelationalTableException e) {
            String message = String.format("Unable to read the definition of table %s as of SCN %s, which is needed " +
                    "to apply its CREATE TABLE AS SELECT: %s", tableId, commitScn, e.getMessage());
            if (isMissingPrivilege(e)) {
                message += " This requires the EXECUTE privilege on DBMS_FLASHBACK for the connector user.";
            }
            switch (connectorConfig.getEventProcessingFailureHandlingMode()) {
                case FAIL -> throw new DebeziumException(message, e);
                case WARN -> {
                    LOGGER.warn("{} The schema change will be skipped.", message, e);
                    streamingMetrics.incrementWarningCount();
                }
                default -> LOGGER.debug("{} The schema change will be skipped.", message, e);
            }
            return null;
        }
    }

    /**
     * Whether the read failed because the connector user lacks the {@code EXECUTE} privilege on
     * {@code DBMS_FLASHBACK}, as opposed to the undo data for the commit no longer being available
     * ({@code ORA-01555}, {@code ORA-08181}) or any other cause, which the error reports as it is.
     */
    private static boolean isMissingPrivilege(Exception e) {
        final String message = e.getMessage();
        return message != null && (message.contains("ORA-01031") || message.contains("PLS-00201") || message.contains("DBMS_FLASHBACK"));
    }

    /**
     * Reads the {@code CREATE TABLE} statement of a table as the database described it at the given system
     * change number. The read happens on a connection of its own, as the adapter's connection is busy with
     * the change stream, and the flashback mode and container switch must not touch that session. The
     * factory's connections reach the primary database when the connector streams from a standby.
     *
     * @param tableId the table identifier, should not be {@code null}
     * @param scn the system change number to read the definition at, should not be {@code null}
     * @return the statement, never {@code null}
     * @throws SQLException if the session cannot be placed in flashback mode or the definition cannot be read
     * @throws NonRelationalTableException if the table is not a relational table
     */
    protected String readTableDdl(TableId tableId, Scn scn) throws SQLException, NonRelationalTableException {
        try (OracleConnection connection = connectionFactory.newConnection()) {
            if (connectorConfig.isUsingPluggableDatabase()) {
                connection.setSessionToPdb(connectorConfig.getPdbName());
            }
            return connection.getTableMetadataDdlAsOfScn(tableId, scn);
        }
    }
}
