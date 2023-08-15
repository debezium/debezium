/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;

import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Clock;

/**
 * Contract that defines unique behavior for each possible {@code connection.adapter}.
 *
 * @author Chris Cranford
 */
public interface StreamingAdapter {

    /**
     * Controls whether table names are viewed as case-sensitive or not.
     */
    enum TableNameCaseSensitivity {
        /**
         * Sensitive case implies that the table names are taken from the JDBC driver and kept as-is
         * in the in-memory relational objects.  Any {@link TableId} that is obtained will always
         * have a table-name in the case that the driver provided.  This is the default behavior
         * for almost all cases.
         */
        SENSITIVE,

        /**
         * Insensitive case implies that the table names are taken from the JDBC driver and converted
         * to lower-case in the in-memory relational objects.  Any {@link TableId} that is obtained
         * will always have a table-name in lower case regardless of how it may be represented in
         * the database.
         */
        INSENSITIVE
    };

    String getType();

    HistoryRecordComparator getHistoryRecordComparator();

    OffsetContext.Loader<OracleOffsetContext> getOffsetContextLoader();

    StreamingChangeEventSource<OraclePartition, OracleOffsetContext> getSource(OracleConnection connection,
                                                                               EventDispatcher<OraclePartition, TableId> dispatcher,
                                                                               ErrorHandler errorHandler, Clock clock,
                                                                               OracleDatabaseSchema schema,
                                                                               OracleTaskContext taskContext,
                                                                               Configuration jdbcConfig,
                                                                               OracleStreamingChangeEventSourceMetrics streamingMetrics);

    /**
     * Returns whether table names are case sensitive.
     *
     * By default the Oracle driver returns table names that are case sensitive.  The table names will
     * be returned in upper-case by default and will only be returned in lower or mixed case when the
     * table is created using double-quotes to preserve case.  The adapter aligns with the driver's
     * behavior and enforces that table names are case sensitive by default.
     *
     * @param connection database connection, should never be {@code null}
     * @return the case sensitivity setting for table names used by the connector's runtime adapter
     */
    default TableNameCaseSensitivity getTableNameCaseSensitivity(OracleConnection connection) {
        return TableNameCaseSensitivity.SENSITIVE;
    }

    /**
     * Returns the offset context based on the snapshot state.
     *
     * @param ctx the relational snapshot context, should never be {@code null}
     * @param connectorConfig the connector configuration, should never be {@code null}
     * @param connection the database connection, should never be {@code null}
     * @return the offset context, never {@code null}
     * @throws SQLException if a database error occurred
     */
    OracleOffsetContext determineSnapshotOffset(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx,
                                                OracleConnectorConfig connectorConfig, OracleConnection connection)
            throws SQLException;

    /**
     * Returns the value converter for the streaming adapter.
     *
     * @param connectorConfig the connector configuration, shoudl never be {@code null}
     * @param connection the database connection, should never be {@code null}
     * @return the value converter to be used
     */
    default OracleValueConverters getValueConverter(OracleConnectorConfig connectorConfig, OracleConnection connection) {
        return new OracleValueConverters(connectorConfig, connection);
    }

}
