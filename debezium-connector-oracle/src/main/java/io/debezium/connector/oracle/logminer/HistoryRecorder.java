/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigDecimal;
import java.sql.Timestamp;

import io.debezium.common.annotation.Incubating;
import io.debezium.jdbc.JdbcConfiguration;

/**
 * This interface defines how a custom recorder can be supplied to record LogMiner results.
 */
@Incubating
public interface HistoryRecorder {
    /**
     * Prepares the history recorder
     *
     * @param metrics the log miner jmx metrics
     * @param jdbcConfiguration the jdbc configuration
     * @param retentionHours the history retention hours
     */
    void prepare(LogMinerMetrics metrics, JdbcConfiguration jdbcConfiguration, long retentionHours);

    /**
     * Records the LogMiner entry.
     *
     * @param scn the entry's SCN
     * @param tableName the table name
     * @param segOwner the table owner
     * @param operationCode the operation code
     * @param changeTime the time the operation occurred
     * @param transactionId the transaction identifier
     * @param csf the continuation sequence flag
     * @param redoSql the redo SQL that performed the operation
     */
    void record(BigDecimal scn, String tableName, String segOwner, int operationCode, Timestamp changeTime,
                String transactionId, int csf, String redoSql);

    /**
     * Flushes the LogMiner history captured by the recorder.
     */
    void flush();

    /**
     * Closes the LogMiner history recorder.
     */
    void close();
}
