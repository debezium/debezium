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
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGlseg;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
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
    private static final Pattern TYPE_PATTERN = Pattern.compile("^(?<full>(?<base>[^(\\[]+)(?:\\((?<mod>.+)\\))?(?<suffix>.*?))(?<array>\\[\\])?$");

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
        if (rawValue.isNull()) {
            // nulls are null
            return null;
        }

        // we support wal2json both before & after commit 0255f2ac, which means we may get old-style type names
        // (eg. int2, timetz, varchar, numeric, _int4)
        // or new-style/verbose ones
        // (eg. smallint; time with time zone; character varying (255); decimal (10,3), integer[])

        Matcher m = TYPE_PATTERN.matcher(columnType);
        if (!m.matches()) {
            LOGGER.error("Failed to parse columnType for {} '{}'", columnName, columnType);
            throw new ConnectException(String.format("Failed to parse columnType '%s' for column %s", columnType, columnName));
        }
        String fullType = m.group("full");
        String baseType = m.group("base").trim();
        if (!Objects.toString(m.group("suffix"), "").isEmpty()) {
            baseType = String.join(" ", baseType, m.group("suffix").trim());
        }

        boolean isArray = (m.group("array") != null);

        if (baseType.startsWith("_")) {
            // old-style type specifiers use an _ prefix for arrays
            // e.g. int4[] would be "_int4"
            baseType = baseType.substring(1);
            fullType = fullType.substring(1);
            isArray = true;
        }

        if (isArray) {
            try {
                final String dataString = rawValue.asString();
                PgArray arrayData = new PgArray(connection.get(), connection.get().getTypeInfo().getPGArrayType(fullType), dataString);
                Object deserializedArray = arrayData.getArray();
                // TODO: what types are these? Shouldn't they pass through this function again?
                return Arrays.asList((Object[])deserializedArray);
            }
            catch (SQLException e) {
                LOGGER.warn("Unexpected exception trying to process PgArray ({}) column '{}', {}", columnType, columnName, e);
            }
            return null;
        }

        switch (baseType) {
            // include all types from https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
            // plus aliases from the shorter names produced by older wal2json
            case "boolean":
            case "bool":
                return rawValue.asBoolean();

            case "integer":
            case "int":
            case "int4":
            case "smallint":
            case "int2":
            case "smallserial":
            case "serial":
            case "serial2":
            case "serial4":
            case "oid":
                return rawValue.asInteger();

            case "bigint":
            case "bigserial":
            case "int8":
                return rawValue.asLong();

            case "real":
            case "float4":
                return rawValue.asFloat();

            case "double precision":
            case "float8":
                return rawValue.asDouble();

            case "numeric":
            case "decimal":
                // TODO: Support for Decimal/Numeric types with correct scale+precision
                return rawValue.asDouble();

            case "character":
            case "char":
            case "character varying":
            case "varchar":
            case "bpchar":
            case "text":
                return rawValue.asString();

            case "date":
                return DateTimeFormat.get().date(rawValue.asString());

            case "timestamp with time zone":
            case "timestamptz":
                return DateTimeFormat.get().timestampWithTimeZone(rawValue.asString());

            case "timestamp":
            case "timestamp without time zone":
                return DateTimeFormat.get().timestamp(rawValue.asString());

            case "time":
            case "time without time zone":
                return DateTimeFormat.get().time(rawValue.asString());

            case "time with time zone":
            case "timetz":
                return DateTimeFormat.get().timeWithTimeZone(rawValue.asString());

            case "bytea":
                return Strings.hexStringToByteArray(rawValue.asString());

            // these are all PG-specific types and we use the JDBC representations
            // note that, with the exception of point, no converters for these types are implemented yet,
            // i.e. those values won't actually be propagated to the outbound message until that's the case
            case "box":
                try {
                    return new PGbox(rawValue.asString());
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse point {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "circle":
                try {
                    return new PGcircle(rawValue.asString());
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse circle {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "interval":
                try {
                    return new PGInterval(rawValue.asString());
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse point {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "line":
                try {
                    return new PGline(rawValue.asString());
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse point {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "lseg":
                try {
                    return new PGlseg(rawValue.asString());
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse point {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "money":
                try {
                    return new PGmoney(rawValue.asString()).val;
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse money {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "path":
                try {
                    return new PGpath(rawValue.asString());
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse point {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "point":
                try {
                    return new PGpoint(rawValue.asString());
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse point {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }
            case "polygon":
                try {
                    return new PGpolygon(rawValue.asString());
                } catch (final SQLException e) {
                    LOGGER.error("Failed to parse point {}, {}", rawValue.asString(), e);
                    throw new ConnectException(e);
                }

            case "bit":
            case "bit varying":
            case "varbit":
            case "json":
            case "jsonb":
            case "xml":
            case "uuid":
            case "tstzrange":
                return rawValue.asString();
            // catch-all for other known/builtin PG types
            // TODO: improve with more specific/useful classes here?
            case "cidr":
            case "inet":
            case "macaddr":
            case "macaddr8":
            case "pg_lsn":
            case "tsquery":
            case "tsvector":
            case "txid_snapshot":
            // catch-all for unknown (extension module/custom) types
            default:
                break;
        }

        return null;
    }
}
