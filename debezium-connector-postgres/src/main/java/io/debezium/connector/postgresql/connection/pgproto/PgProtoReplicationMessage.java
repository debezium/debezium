/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection.pgproto;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PgOid;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.UnchangedToastedReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.time.Conversions;
import io.debezium.util.Strings;

/**
 * Replication message representing message sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</>
 *
 * @author Jiri Pechanec
 */
class PgProtoReplicationMessage implements ReplicationMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgProtoReplicationMessage.class);

    private final PgProto.RowMessage rawMessage;
    private final TypeRegistry typeRegistry;

    public PgProtoReplicationMessage(PgProto.RowMessage rawMessage, TypeRegistry typeRegistry) {
        this.rawMessage = rawMessage;
        this.typeRegistry = typeRegistry;
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
    public Instant getCommitTime() {
        // value is microseconds
        return Instant.ofEpochSecond(0, rawMessage.getCommitTime() * 1_000);
    }

    @Override
    public long getTransactionId() {
        return Integer.toUnsignedLong(rawMessage.getTransactionId());
    }

    @Override
    public String getTable() {
        return rawMessage.getTable();
    }

    @Override
    public List<ReplicationMessage.Column> getOldTupleList() {
        return transform(rawMessage.getOldTupleList(), null);
    }

    @Override
    public List<ReplicationMessage.Column> getNewTupleList() {
        return transform(rawMessage.getNewTupleList(), rawMessage.getNewTypeinfoList());
    }

    @Override
    public boolean hasTypeMetadata() {
        return !(rawMessage.getNewTypeinfoList() == null || rawMessage.getNewTypeinfoList().isEmpty());
    }

    private List<ReplicationMessage.Column> transform(List<PgProto.DatumMessage> messageList, List<PgProto.TypeInfo> typeInfoList) {
        return IntStream.range(0, messageList.size())
                .mapToObj(index -> {
                    final PgProto.DatumMessage datum = messageList.get(index);
                    final Optional<PgProto.TypeInfo> typeInfo = Optional.ofNullable(hasTypeMetadata() && typeInfoList != null ? typeInfoList.get(index) : null);
                    final String columnName = Strings.unquoteIdentifierPart(datum.getColumnName());
                    final PostgresType type = typeRegistry.get((int) datum.getColumnType());
                    if (datum.hasDatumMissing()) {
                        return new UnchangedToastedReplicationMessageColumn(columnName, type, typeInfo.map(PgProto.TypeInfo::getModifier).orElse(null), typeInfo.map(PgProto.TypeInfo::getValueOptional).orElse(Boolean.FALSE), hasTypeMetadata());
                    }
                    return new AbstractReplicationMessageColumn(columnName, type, typeInfo.map(PgProto.TypeInfo::getModifier).orElse(null), typeInfo.map(PgProto.TypeInfo::getValueOptional).orElse(Boolean.FALSE), hasTypeMetadata()) {

                        @Override
                        public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                            return PgProtoReplicationMessage.this.getValue(datum, connection, includeUnknownDatatypes);
                        }

                        @Override
                        public String toString() {
                            return datum.toString();
                        }
                    };
                   })
                .collect(Collectors.toList());
    }


    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    /**
     * Converts the Protobuf value for a {@link io.debezium.connector.postgresql.proto.PgProto.DatumMessage plugin message} to
     * a Java value based on the type of the column from the message. This value will be converted later on if necessary by the
     * {@link PostgresValueConverter#converter(Column, Field)} instance to match whatever the Connect schema type expects.
     *
     * Note that the logic here is tightly coupled (i.e. dependent) on the Postgres plugin logic which writes the actual
     * Protobuf messages.
     *
     * @param a supplier to get a connection to Postgres instance for array handling
     * @return the value; may be null
     */
    public Object getValue(PgProto.DatumMessage datumMessage, PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
        if (datumMessage.hasDatumMissing()) {
            return UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE;
        }

        int columnType = (int) datumMessage.getColumnType();
        switch (columnType) {
            case PgOid.BOOL:
                return datumMessage.hasDatumBool() ? datumMessage.getDatumBool() : null;
            case PgOid.INT2:
            case PgOid.INT4:
                return datumMessage.hasDatumInt32() ? datumMessage.getDatumInt32() : null;
            case PgOid.INT8:
            case PgOid.OID:
            case PgOid.MONEY:
                return datumMessage.hasDatumInt64() ? datumMessage.getDatumInt64() : null;
            case PgOid.FLOAT4:
                return datumMessage.hasDatumFloat() ? datumMessage.getDatumFloat() : null;
            case PgOid.FLOAT8:
                return datumMessage.hasDatumDouble() ? datumMessage.getDatumDouble() : null;
            case PgOid.NUMERIC:
                if (datumMessage.hasDatumDouble()) {
                    // For backwards compatibility only to enable independent upgrade of Postgres plug-in
                    return datumMessage.getDatumDouble();
                }
                else if (datumMessage.hasDatumString()) {
                    final String s = datumMessage.getDatumString();
                    return PostgresValueConverter.toSpecialValue(s).orElseGet(() -> new SpecialValueDecimal(new BigDecimal(s)));
                }
                return null;
            case PgOid.CHAR:
            case PgOid.VARCHAR:
            case PgOid.BPCHAR:
            case PgOid.TEXT:
            case PgOid.JSON:
            case PgOid.JSONB_OID:
            case PgOid.XML:
            case PgOid.UUID:
            case PgOid.BIT:
            case PgOid.VARBIT:
            case PgOid.INET_OID:
            case PgOid.CIDR_OID:
            case PgOid.MACADDR_OID:
            case PgOid.MACADDR8_OID:
                return datumMessage.hasDatumString() ? datumMessage.getDatumString() : null;
            case PgOid.DATE:
                return datumMessage.hasDatumInt32() ? (long) datumMessage.getDatumInt32() : null;
            case PgOid.TIMESTAMP:
                if (!datumMessage.hasDatumInt64()) {
                    return null;
                }
                // these types are sent by the plugin as LONG - microseconds since Unix Epoch
                return Conversions.toInstantFromMicros(datumMessage.getDatumInt64());
            case PgOid.TIMESTAMPTZ:
                if (!datumMessage.hasDatumInt64()) {
                    return null;
                }
                // these types are sent by the plugin as LONG - microseconds since Unix Epoch
                return Conversions.toInstantFromMicros(datumMessage.getDatumInt64()).atOffset(ZoneOffset.UTC);
            case PgOid.TIME:
                if (!datumMessage.hasDatumInt64()) {
                    return null;
                }

                // these types are sent by the plugin as LONG - microseconds since Unix Epoch
                return Duration.of(datumMessage.getDatumInt64(), ChronoUnit.MICROS);
            case PgOid.TIMETZ:
                if (!datumMessage.hasDatumDouble()) {
                    return null;
                }
                // the value is sent as a double microseconds
                return Conversions.toInstantFromMicros((long) datumMessage.getDatumDouble())
                        .atOffset(ZoneOffset.UTC)
                        .toOffsetTime();
            case PgOid.INTERVAL:
                // these are sent as doubles by the plugin since their storage is larger than 8 bytes
            try {
                return datumMessage.hasDatumDouble() ? datumMessage.getDatumDouble() :
                    datumMessage.hasDatumString() ? new PGInterval(datumMessage.getDatumString()) : null;
            }
            catch (SQLException e) {
                throw new ConnectException("Could not convert interval value");
            }
            // the plugin will send back a TZ formatted string
            case PgOid.BYTEA:
                return datumMessage.hasDatumBytes() ? datumMessage.getDatumBytes().toByteArray() : null;
            case PgOid.POINT: {
                PgProto.Point datumPoint = datumMessage.getDatumPoint();
                return new PGpoint(datumPoint.getX(), datumPoint.getY());
            }
            case PgOid.TSRANGE_OID:
            case PgOid.TSTZRANGE_OID:
            case PgOid.DATERANGE_OID:
            case PgOid.INT4RANGE_OID:
            case PgOid.NUM_RANGE_OID:
            case PgOid.INT8RANGE_OID:
                return datumMessage.hasDatumBytes() ? new String(datumMessage.getDatumBytes().toByteArray(), Charset.forName("UTF-8")) : null;
            case PgOid.INT2_ARRAY:
            case PgOid.INT4_ARRAY:
            case PgOid.INT8_ARRAY:
            case PgOid.TEXT_ARRAY:
            case PgOid.NUMERIC_ARRAY:
            case PgOid.FLOAT4_ARRAY:
            case PgOid.FLOAT8_ARRAY:
            case PgOid.BOOL_ARRAY:
            case PgOid.DATE_ARRAY:
            case PgOid.TIME_ARRAY:
            case PgOid.TIMETZ_ARRAY:
            case PgOid.TIMESTAMP_ARRAY:
            case PgOid.TIMESTAMPTZ_ARRAY:
            case PgOid.BYTEA_ARRAY:
            case PgOid.VARCHAR_ARRAY:
            case PgOid.OID_ARRAY:
            case PgOid.BPCHAR_ARRAY:
            case PgOid.MONEY_ARRAY:
            case PgOid.NAME_ARRAY:
            case PgOid.INTERVAL_ARRAY:
            case PgOid.CHAR_ARRAY:
            case PgOid.VARBIT_ARRAY:
            case PgOid.UUID_ARRAY:
            case PgOid.XML_ARRAY:
            case PgOid.POINT_ARRAY:
            case PgOid.JSONB_ARRAY:
            case PgOid.JSON_ARRAY:
            case PgOid.REF_CURSOR_ARRAY:
            case PgOid.INET_ARRAY:
            case PgOid.CIDR_ARRAY:
            case PgOid.MACADDR_ARRAY:
            case PgOid.MACADDR8_ARRAY:
            case PgOid.TSRANGE_ARRAY:
            case PgOid.TSTZRANGE_ARRAY:
            case PgOid.DATERANGE_ARRAY:
            case PgOid.INT4RANGE_ARRAY:
            case PgOid.NUM_RANGE_ARRAY:
            case PgOid.INT8RANGE_ARRAY:
            return getArray(datumMessage, connection, columnType);

            case PgOid.UNSPECIFIED:
                return null;

            default:
                PostgresType type = typeRegistry.get(columnType);
                if (type.getOid() == typeRegistry.geometryOid() || type.getOid() == typeRegistry.geographyOid() || type.getOid() == typeRegistry.citextOid() ) {
                    return datumMessage.getDatumBytes().toByteArray();
                }
                if (type.getOid() == typeRegistry.hstoreOid()) {
                    return datumMessage.getDatumBytes().toByteArray();
                }
                if (type.getOid() == typeRegistry.geometryArrayOid() ||
                        type.getOid() == typeRegistry.geographyArrayOid() ||
                        type.getOid() == typeRegistry.citextArrayOid() ||
                        type.getOid() == typeRegistry.hstoreArrayOid()) {
                    return getArray(datumMessage, connection, columnType);
                }
                // unknown data type is sent by decoder as binary value
                if (includeUnknownDatatypes && datumMessage.hasDatumBytes()) {
                    return datumMessage.getDatumBytes().toByteArray();
                }

                return null;
        }
    }

    private Object getArray(PgProto.DatumMessage datumMessage, PgConnectionSupplier connection, int columnType) {
        // Currently the logical decoding plugin sends unhandled types as a byte array containing the string
        // representation (in Postgres) of the array value.
        // The approach to decode this is sub-optimal but the only way to improve this is to update the plugin.
        // Reasons for it being sub-optimal include:
        // 1. It requires a Postgres JDBC connection to deserialize
        // 2. The byte-array is a serialised string but we make the assumption its UTF-8 encoded (which it will
        //    be in most cases)
        // 3. For larger arrays and especially 64-bit integers and the like it is less efficient sending string
        //    representations over the wire.
        try {
            byte[] data = datumMessage.hasDatumBytes() ? datumMessage.getDatumBytes().toByteArray() : null;
            if (data == null) {
                return null;
            }
            String dataString = new String(data, Charset.forName("UTF-8"));
            PgArray arrayData = new PgArray(connection.get(), columnType, dataString);
            Object deserializedArray = arrayData.getArray();
            return Arrays.asList((Object[]) deserializedArray);
        }
        catch (SQLException e) {
            LOGGER.warn("Unexpected exception trying to process PgArray column '{}'", datumMessage.getColumnName(), e);
        }
        return null;
    }
}
