/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;

import io.debezium.jdbc.JdbcConnection;

/**
 * A centralized expression of differences in behaviour between MySQL 5.x and 8.x
 *
 * @author Jiri Pechanec
 */
public interface DatabaseDifferences {

    boolean isCurrentDateTimeDefaultGenerated();

    String currentDateTimeDefaultOptional(String isoString);

    default void setBinlogRowQueryEventsOff(JdbcConnection connection) throws SQLException {
        connection.execute("SET binlog_rows_query_log_events=OFF");
    }

    default void setBinlogRowQueryEventsOn(JdbcConnection connection) throws SQLException {
        connection.execute("SET binlog_rows_query_log_events=ON");
    }

    default void setBinlogCompressionOff(JdbcConnection connection) throws SQLException {
        connection.execute("set binlog_transaction_compression=OFF;");
    }

    default void setBinlogCompressionOn(JdbcConnection connection) throws SQLException {
        connection.execute("set binlog_transaction_compression=ON;");
    }
}
