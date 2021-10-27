/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.time.Instant;
import java.util.List;

public class LogicalDecodingMessage implements ReplicationMessage {
    private final Operation operation;
    private final Instant commitTime;
    private final long transactionId;
    private final boolean isTransactional;
    private final String prefix;
    private final byte[] content;

    public LogicalDecodingMessage(Operation op, Instant commitTimestamp, long transactionId, boolean isTransactional,
                                  String prefix, byte[] content) {
        this.operation = op;
        this.commitTime = commitTimestamp;
        this.transactionId = transactionId;
        this.isTransactional = isTransactional;
        this.prefix = prefix;
        this.content = content;
    }

    @Override
    public boolean isLastEventForLsn() {
        return !isTransactional;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    public Instant getCommitTime() {
        return commitTime;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public String getTable() {
        throw new UnsupportedOperationException();
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
    public boolean hasTypeMetadata() {
        throw new UnsupportedOperationException();
    }

    public String getPrefix() {
        return prefix;
    }

    public byte[] getContent() {
        return content;
    }

    public boolean isTransactional() {
        return isTransactional;
    }
}
