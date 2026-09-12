/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * A LogMiner event with SEQUENCE#.
 *
 * @author Sergei Nikolaev
 */
public class LogMinerEventWithSequence extends LogMinerEvent {
    private final Long transactionSequence;

    public LogMinerEventWithSequence(LogMinerEventRow row) {
        super(row);
        this.transactionSequence = row.getTransactionSequence();
    }

    public LogMinerEventWithSequence(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime, Long transactionSequence) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
        this.transactionSequence = transactionSequence;
    }

    public Long getTransactionSequence() {
        return transactionSequence;
    }
}
