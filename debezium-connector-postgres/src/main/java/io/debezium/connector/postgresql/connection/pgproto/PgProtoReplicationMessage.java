/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection.pgproto;

import java.util.List;
import java.util.stream.Collectors;

import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.proto.PgProto;

/**
 * Replication message representing message sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</>
 * 
 * @author Jiri Pechanec
 *
 */
class PgProtoReplicationMessage implements ReplicationMessage {
    private final PgProto.RowMessage rawMessage;

    public PgProtoReplicationMessage(final PgProto.RowMessage rawMessage) {
        this.rawMessage = rawMessage;
    }

    @Override
    public Operation getOperation() {
        switch (rawMessage.getOp()) {
        case INSERT:
            return Operation.INSERT;
        case UPDATE:
            return Operation.UPDATE;
        case DELETE:
            return Operation.DELETE;
        }
        throw new IllegalArgumentException(
                "Unknown operation '" + rawMessage.getOp() + "' in replication stream message");
    }

    @Override
    public long getCommitTime() {
        return rawMessage.getCommitTime();
    }

    @Override
    public int getTransactionId() {
        return rawMessage.getTransactionId();
    }

    @Override
    public String getTable() {
        return rawMessage.getTable();
    }

    @Override
    public List<ReplicationMessage.Column> getOldTupleList() {
        return transform(rawMessage.getOldTupleList());
    }

    @Override
    public List<ReplicationMessage.Column> getNewTupleList() {
        return transform(rawMessage.getNewTupleList());
    }

    private List<ReplicationMessage.Column> transform(List<PgProto.DatumMessage> messageList) {
        return messageList.stream()
                .map(PgProtoColumn::new)
                .collect(Collectors.toList());
    }
}
