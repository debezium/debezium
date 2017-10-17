/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection.wal2json;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.document.Array;
import io.debezium.document.Document;

/**
 * Replication message representing message sent by the wal2json logical decoding plug-in.
 *
 * @author Jiri Pechanec
 */
class Wal2JsonReplicationMessage implements ReplicationMessage {

    private final int txId;
    private final long commitTime;
    private final Document rawMessage;

    public Wal2JsonReplicationMessage(final int txId, final long commitTime, final Document rawMessage) {
        this.txId = txId;
        this.commitTime = commitTime;
        this.rawMessage = rawMessage;
    }

    @Override
    public Operation getOperation() {
        final String operation = rawMessage.getString("kind");
        switch (operation) {
            case "insert":
                return Operation.INSERT;
            case "update":
                return Operation.UPDATE;
            case "delete":
                return Operation.DELETE;
        }
        throw new IllegalArgumentException(
                "Unknown operation '" + operation + "' in replication stream message");
    }

    @Override
    public long getCommitTime() {
        return commitTime;
    }

    @Override
    public int getTransactionId() {
        return txId;
    }

    @Override
    public String getTable() {
        return "\"" + rawMessage.getString("schema") + "\".\"" + rawMessage.getString("table") + "\"";
    }

    @Override
    public List<ReplicationMessage.Column> getOldTupleList() {
        final Document oldkeys = rawMessage.getDocument("oldkeys");
        return oldkeys != null ? transform(oldkeys, "keynames", "keytypes", "keyvalues") : null;
    }

    @Override
    public List<ReplicationMessage.Column> getNewTupleList() {
        return transform(rawMessage, "columnnames", "columntypes", "columnvalues");
    }

    private List<ReplicationMessage.Column> transform(final Document data, final String nameField, final String typeField, final String valueField) {
        final Array columnNames = data.getArray(nameField);
        final Array columnTypes = data.getArray(typeField);
        final Array columnValues = data.getArray(valueField);

        if (columnNames.size() != columnTypes.size() || columnNames.size() != columnValues.size()) {
            throw new ConnectException("Column related arrays do not have the same size");
        }

        final List<ReplicationMessage.Column> columns = new ArrayList<>(columnNames.size());

        for (int i = 0; i < columnNames.size(); i++) {
            columns.add(new Wal2JsonColumn(columnNames.get(i).asString(), columnTypes.get(i).asString(), columnValues.get(i)));
        }

        return columns;
    }
}
