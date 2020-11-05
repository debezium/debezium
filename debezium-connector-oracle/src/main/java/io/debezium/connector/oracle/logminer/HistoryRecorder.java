/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigDecimal;
import java.sql.Timestamp;

public interface HistoryRecorder {
    void insertIntoTempTable(BigDecimal scn, String tableName, String segOwner, int operationCode, Timestamp changeTime,
                             String transactionId, int csf, String redoSql);

    void doBulkHistoryInsert();
}
