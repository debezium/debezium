/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection.wal2json;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.core.Oid;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGmoney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.RecordsStreamProducer.PgConnectionSupplier;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.Value;
import io.debezium.util.Strings;

/**
 * Replication message representing message sent by the wal2json logical decoding plug-in.
 *
 * @author Jiri Pechanec
 */
class Wal2JsonReplicationMessage implements ReplicationMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(Wal2JsonReplicationMessage.class);

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
            String columnName = columnNames.get(i).asString();
            String columnType = columnTypes.get(i).asString();
            Value rawValue = columnValues.get(i);

            columns.add(new ReplicationMessage.Column() {

                @Override
                public Object getValue(PgConnectionSupplier connection) {
                    return Wal2JsonReplicationMessage.this.getValue(columnName, columnType, rawValue, connection);
                }

                @Override
                public String getType() {
                    return columnType;
                }

                @Override
                public String getName() {
                    return columnName;
                }
            });
        }

        return columns;
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
    public Object getValue(String columnName, String columnType, Value rawValue, final PgConnectionSupplier connection) {
        switch (columnType) {
            case "bool":
                return rawValue.isNotNull() ? rawValue.asBoolean() : null;
            case "int2":
            case "int4":
                return rawValue.isNotNull() ? rawValue.asInteger() : null;
            case "int8":
            case "oid":
                return rawValue.isNotNull() ? rawValue.asLong() : null;
            case "float4":
                return rawValue.isNotNull() ? rawValue.asFloat() : null;
            case "float8":
                return rawValue.isNotNull() ? rawValue.asDouble() : null;
            case "numeric":
                return rawValue.isNotNull() ? rawValue.asDouble() : null;
            case "char":
            case "varchar":
            case "bpchar":
            case "text":
            case "json":
            case "jsonb":
            case "xml":
            case "uuid":
            case "bit":
            case "varbit":
            case "tstzrange":
                return rawValue.isNotNull() ? rawValue.asString() : null;
            case "date":
                return rawValue.isNotNull() ? DateTimeFormat.get().date(rawValue.asString()) : null;
            case "timestamp":
                return rawValue.isNotNull() ? DateTimeFormat.get().timestamp(rawValue.asString()) : null;
            case "timestamptz":
                return rawValue.isNotNull() ? DateTimeFormat.get().timestampWithTimeZone(rawValue.asString()) : null;
            case "time":
                return rawValue.isNotNull() ? DateTimeFormat.get().time(rawValue.asString()) : null;
            case "timetz":
                return rawValue.isNotNull() ?  DateTimeFormat.get().timeWithTimeZone(rawValue.asString()) : null;
            case "bytea":
            return Strings.hexStringToByteArray(rawValue.asString());
            case "point":
                try {
                    return rawValue.isNotNull() ? new PGpoint(rawValue.asString()) : null;
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse point {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "money":
                try {
                    return rawValue.isNotNull() ? new PGmoney(rawValue.asString()).val : null;
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse money {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "interval":
                try {
                    return rawValue.isNotNull() ? new PGInterval(rawValue.asString()) : null;
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse point {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "_int2":
            case "_int4":
            case "_int8":
            case "_text":
            case "_numeric":
            case "_float4":
            case "_float8":
            case "_bool":
            case "_date":
            case "_time":
            case "_timetz":
            case "_timestamp":
            case "_timestamptz":
            case "_bytea":
            case "_varchar":
            case "_oid":
            case "_bpchar":
            case "_money":
            case "_name":
            case "_interval":
            case "_char":
            case "_varbit":
            case "_uuid":
            case "_xml":
            case "_point":
            case "_jsonb":
            case "_json":
            case "_ref_cursor":
                try {
                    final String dataString = rawValue.asString();
                    PgArray arrayData = new PgArray(connection.get(), Oid.valueOf(columnType.substring(1) + "_array"), dataString);
                    Object deserializedArray = arrayData.getArray();
                    return Arrays.asList((Object[])deserializedArray);
                }
                catch (SQLException e) {
                    LOGGER.warn("Unexpected exception trying to process PgArray column '{}'", columnName, e);
                }
                return null;
        }
        LOGGER.warn("processing column '{}' with unknown data type '{}' as byte array", columnName,
                columnType);
        return rawValue.asBytes();
    }
}
