/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.time.Instant;
import java.util.List;

/**
 * Replication message instance representing transaction demarcation events.
 *
 * @author Jiri Pechanec
 *
 */
public class TransactionMessage implements ReplicationMessage {

    private final long transationId;
    private final Instant commitTime;
    private final Operation operation;

    public TransactionMessage(Operation operation, long transactionId, Instant commitTime) {
        this.operation = operation;
        this.transationId = transactionId;
        this.commitTime = commitTime;
    }

    @Override
    public boolean isLastEventForLsn() {
        return operation == Operation.COMMIT;
    }

    @Override
    public boolean hasTypeMetadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTransactionId() {
        return transationId;
    }

    @Override
    public String getTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    public List<Column> getOldTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Column> getNewTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant getCommitTime() {
        return commitTime;
    }

    @Override
    public String toString() {
        return "TransactionMessage [transationId=" + transationId + ", commitTime=" + commitTime + ", operation="
                + operation + "]";
    }
}
