/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

/**
 * Replication message instance representing transaction demarcation events.
 *
 * @author Jiri Pechanec
 *
 */
public class TransactionMessage implements ReplicationMessage {

    private final Long transationId;
    private final Instant commitTime;
    private final Operation operation;

    public TransactionMessage(Operation operation, Long transactionId, Instant commitTime) {
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
    public OptionalLong getTransactionId() {
        return transationId == null ? OptionalLong.empty() : OptionalLong.of(transationId);
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
