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
 * Replication message instance representing a generic logical decoding message
 *
 * @author Lairen Hightower
 */
public class LogicalDecodingMessage implements ReplicationMessage {
    private final Operation operation;
    private final Instant commitTime;
    private final Long transactionId;
    private final boolean isTransactional;
    private final String prefix;
    private final byte[] content;

    public LogicalDecodingMessage(Operation op, Instant commitTimestamp, Long transactionId, boolean isTransactional,
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
    public OptionalLong getTransactionId() {
        return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
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

    public String getPrefix() {
        return prefix;
    }

    public byte[] getContent() {
        return content;
    }
}
