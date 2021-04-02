/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.Timestamp;

import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConfiguration;

/**
 * A history recorder implementation that does not do any recording.
 *
 * @author Chris Cranford
 */
public class NeverHistoryRecorder implements HistoryRecorder {
    @Override
    public void prepare(OracleStreamingChangeEventSourceMetrics streamingMetrics, JdbcConfiguration jdbcConfiguration, long retentionHours) {
    }

    @Override
    public void record(Scn scn, String tableName, String segOwner, int operationCode, Timestamp changeTime,
                       String transactionId, int csf, String redoSql) {
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
}
