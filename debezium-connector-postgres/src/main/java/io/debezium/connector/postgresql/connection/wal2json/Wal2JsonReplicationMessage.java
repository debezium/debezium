/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection.wal2json;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessageColumnValueResolver;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.Value;

/**
 * Replication message representing message sent by the wal2json logical decoding plug-in.
 *
 * @author Jiri Pechanec
 */
class Wal2JsonReplicationMessage implements ReplicationMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(Wal2JsonReplicationMessage.class);

    private final long txId;
    private final Instant commitTime;
    private final Document rawMessage;
    private final boolean hasMetadata;
    private final boolean lastEventForLsn;
    private final TypeRegistry typeRegistry;

    public Wal2JsonReplicationMessage(long txId, Instant commitTime, Document rawMessage, boolean hasMetadata, boolean lastEventForLsn, TypeRegistry typeRegistry) {
        this.txId = txId;
        this.commitTime = commitTime;
        this.rawMessage = rawMessage;
        this.hasMetadata = hasMetadata;
        this.lastEventForLsn = lastEventForLsn;
        this.typeRegistry = typeRegistry;
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
    public Instant getCommitTime() {
        return commitTime;
    }

    @Override
    public long getTransactionId() {
        return txId;
    }

    @Override
    public String getTable() {
        return "\"" + rawMessage.getString("schema") + "\".\"" + rawMessage.getString("table") + "\"";
    }

    @Override
    public List<ReplicationMessage.Column> getOldTupleList() {
        final Document oldkeys = rawMessage.getDocument("oldkeys");
        return oldkeys != null ? transform(oldkeys, "keynames", "keytypes", "keyvalues", "columnoptionals") : null;
    }

    @Override
    public List<ReplicationMessage.Column> getNewTupleList() {
        return transform(rawMessage, "columnnames", "columntypes", "columnvalues", "columnoptionals");
    }

    @Override
    public boolean hasTypeMetadata() {
        return hasMetadata;
    }

    private List<ReplicationMessage.Column> transform(final Document data, final String nameField, final String typeField, final String valueField,
                                                      final String optionalsField) {
        final Array columnNames = data.getArray(nameField);
        final Array columnTypes = data.getArray(typeField);
        final Array columnValues = data.getArray(valueField);
        final Array columnOptionals = data.getArray(optionalsField);

        if (columnNames.size() != columnTypes.size() || columnNames.size() != columnValues.size()) {
            throw new ConnectException("Column related arrays do not have the same size");
        }

        final List<ReplicationMessage.Column> columns = new ArrayList<>(columnNames.size());

        for (int i = 0; i < columnNames.size(); i++) {
            final String columnName = columnNames.get(i).asString();
            final String columnTypeName = columnTypes.get(i).asString();
            final boolean columnOptional = columnOptionals != null ? columnOptionals.get(i).asBoolean() : false;
            final Value rawValue = columnValues.get(i);
            final PostgresType columnType = typeRegistry.get(parseType(columnName, columnTypeName));

            columns.add(new AbstractReplicationMessageColumn(columnName, columnType, columnTypeName, columnOptional, true) {

                @Override
                public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                    return Wal2JsonReplicationMessage.this.getValue(columnName, columnType, columnTypeName, rawValue, connection, includeUnknownDatatypes);
                }

                @Override
                public String toString() {
                    return columnName + "(" + columnTypeName + ")=" + rawValue;
                }

            });
        }

        return columns;
    }

    private String parseType(String columnName, String typeWithModifiers) {
        Matcher m = AbstractReplicationMessageColumn.TypeMetadataImpl.TYPE_PATTERN.matcher(typeWithModifiers);
        if (!m.matches()) {
            LOGGER.error("Failed to parse columnType for {} '{}'", columnName, typeWithModifiers);
            throw new ConnectException(String.format("Failed to parse columnType '%s' for column %s", typeWithModifiers, columnName));
        }
        String baseType = m.group("base").trim();
        final String suffix = m.group("suffix");
        if (suffix != null) {
            baseType += suffix;
        }
        baseType = TypeRegistry.normalizeTypeName(baseType);
        if (m.group("array") != null) {
            baseType = "_" + baseType;
        }
        return baseType;
    }

    /**
     * Converts the value (string representation) coming from wal2json plugin to
     * a Java value based on the type of the column from the message. This value will be converted later on if necessary by the
     * {@link PostgresValueConverter#converter(Column, Field)} instance to match whatever the Connect schema type expects.
     *
     * Note that the logic here is tightly coupled (i.e. dependent) on the wal2json plugin logic which writes the actual
     * JSON messages.
     * @param a supplier to get a connection to Postgres instance for array handling
     *
     * @return the value; may be null
     */
    public Object getValue(String columnName, PostgresType type, String fullType, Value rawValue, final PgConnectionSupplier connection,
                           boolean includeUnknownDatatypes) {
        final Wal2JsonColumnValue columnValue = new Wal2JsonColumnValue(rawValue);
        return ReplicationMessageColumnValueResolver.resolveValue(columnName, type, fullType, columnValue, connection, includeUnknownDatatypes, typeRegistry);
    }

    @Override
    public boolean isLastEventForLsn() {
        return lastEventForLsn;
    }
}
