/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import static io.debezium.connector.postgresql.PgOid.BIT;
import static io.debezium.connector.postgresql.PgOid.BOOL;
import static io.debezium.connector.postgresql.PgOid.BOX;
import static io.debezium.connector.postgresql.PgOid.BPCHAR;
import static io.debezium.connector.postgresql.PgOid.BYTEA;
import static io.debezium.connector.postgresql.PgOid.CHAR;
import static io.debezium.connector.postgresql.PgOid.CIDR;
import static io.debezium.connector.postgresql.PgOid.CIRCLE;
import static io.debezium.connector.postgresql.PgOid.DATE;
import static io.debezium.connector.postgresql.PgOid.DATERANGE_OID;
import static io.debezium.connector.postgresql.PgOid.FLOAT4;
import static io.debezium.connector.postgresql.PgOid.FLOAT8;
import static io.debezium.connector.postgresql.PgOid.INET;
import static io.debezium.connector.postgresql.PgOid.INT2;
import static io.debezium.connector.postgresql.PgOid.INT4;
import static io.debezium.connector.postgresql.PgOid.INT4RANGE_OID;
import static io.debezium.connector.postgresql.PgOid.INT8;
import static io.debezium.connector.postgresql.PgOid.INT8RANGE_OID;
import static io.debezium.connector.postgresql.PgOid.INTERVAL;
import static io.debezium.connector.postgresql.PgOid.JSON;
import static io.debezium.connector.postgresql.PgOid.JSONB;
import static io.debezium.connector.postgresql.PgOid.LINE;
import static io.debezium.connector.postgresql.PgOid.LSEG;
import static io.debezium.connector.postgresql.PgOid.MACADDR;
import static io.debezium.connector.postgresql.PgOid.MACADDR8;
import static io.debezium.connector.postgresql.PgOid.MONEY;
import static io.debezium.connector.postgresql.PgOid.NUMERIC;
import static io.debezium.connector.postgresql.PgOid.NUM_RANGE_OID;
import static io.debezium.connector.postgresql.PgOid.OID;
import static io.debezium.connector.postgresql.PgOid.PATH;
import static io.debezium.connector.postgresql.PgOid.POINT;
import static io.debezium.connector.postgresql.PgOid.POLYGON;
import static io.debezium.connector.postgresql.PgOid.TEXT;
import static io.debezium.connector.postgresql.PgOid.TIME;
import static io.debezium.connector.postgresql.PgOid.TIMESTAMP;
import static io.debezium.connector.postgresql.PgOid.TIMESTAMPTZ;
import static io.debezium.connector.postgresql.PgOid.TIMETZ;
import static io.debezium.connector.postgresql.PgOid.TSRANGE_OID;
import static io.debezium.connector.postgresql.PgOid.TSTZRANGE_OID;
import static io.debezium.connector.postgresql.PgOid.UUID;
import static io.debezium.connector.postgresql.PgOid.VARBIT;
import static io.debezium.connector.postgresql.PgOid.VARCHAR;
import static io.debezium.connector.postgresql.PgOid.XML;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGlseg;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.PGInterval;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource;
import io.debezium.connector.postgresql.PostgresTimeBoundary;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.data.SpecialValueDecimal;

/**
 * An attribute decoder that decodes PgOutput binary encoded attributes. This requires slot options {@code proto_version = 2} and {@code
 * binary = true}.
 */
class BinaryAttributeDecoder extends AbstractAttributeDecoder {

    private static final Instant PG_EPOCH_INSTANT = LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
    private static final LocalDate PG_EPOCH_DATE = LocalDate.of(2000, 1, 1);

    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long MICROS_PER_MINUTE = MICROS_PER_SECOND * 60L;
    private static final long MICROS_PER_HOUR = MICROS_PER_MINUTE * 60L;

    private static final byte RANGE_EMPTY = 0x01;
    private static final byte RANGE_LOWER_INC = 0x02;
    private static final byte RANGE_UPPER_INC = 0x04;
    private static final byte RANGE_LOWER_INFINITY = 0x08;
    private static final byte RANGE_UPPER_INFINITY = 0x10;

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter TIMESTAMP_TZ_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxx");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withZone(ZoneOffset.UTC);

    BinaryAttributeDecoder() {
    }

    @Override
    public Object decode(final ByteBuffer buffer, final String columnName, final PostgresType type, final String fullType,
                         final PostgresStreamingChangeEventSource.PgConnectionSupplier connection, final boolean includeUnknownDatatypes,
                         final TypeRegistry typeRegistry) {
        final var length = buffer.getInt();
        return decodeValue(buffer, typeRegistry, type, length, includeUnknownDatatypes);
    }

    private Object decodeValue(final ByteBuffer buffer, final TypeRegistry typeRegistry, final PostgresType type,
                               final int length, final boolean includeUnknownDatatypes) {

        // length == -1 means the value is null in arrays only. For top level attributes, the attribute nullness is determined earlier 'n'.
        if (length < 0) {
            return null;
        }

        if (!type.isRootType()) {
            return decodeValue(buffer, typeRegistry, type.getParentType(), length, includeUnknownDatatypes);
        }

        if (type.isArrayType()) {
            return arrayToList(decodeArray(buffer, length, typeRegistry, includeUnknownDatatypes));
        }

        if (type.isEnumType()) {
            return decodeStringOfLength(buffer, length);
        }

        // First we switch on known OIDs, then we switch using type names. This is a micro-optimization as switching on int is faster
        // than switching on strings.
        return switch (type.getOid()) {
            case INT2 -> buffer.getShort();
            case INT4 -> buffer.getInt();
            case INT8 -> buffer.getLong();
            case OID -> Integer.toUnsignedLong(buffer.getInt());
            case BOOL -> buffer.get() == 1;
            case FLOAT4 -> buffer.getFloat();
            case FLOAT8 -> buffer.getDouble();
            case NUMERIC -> decodeNumeric(buffer, length);
            case MONEY -> decodeMoney(buffer, length, typeRegistry);

            case CHAR -> (char) buffer.get();
            case VARCHAR, TEXT, JSON, BPCHAR, XML -> decodeStringOfLength(buffer, length);
            case JSONB -> decodeJsonb(buffer, length);

            case BYTEA -> decodeByteArray(buffer, length);
            case BIT, VARBIT -> decodeBit(buffer, length);

            case TIMESTAMP -> decodeTimestamp(buffer, length);
            case TIMESTAMPTZ -> decodeZonedTimestamp(buffer, length);
            case DATE -> decodeDate(buffer, length);
            case TIME -> decodeTimeAsDuration(buffer, length);
            case TIMETZ -> decodeZonedTime(buffer, length);
            case INTERVAL -> decodeInterval(buffer, length);

            case UUID -> new UUID(buffer.getLong(), buffer.getLong());

            case INET, CIDR -> decodeInet(buffer, length);
            case MACADDR -> decodeMacAddr(buffer, length, 6);
            case MACADDR8 -> decodeMacAddr(buffer, length, 8);

            case POINT -> decodePoint(buffer, length);
            case POLYGON -> decodePolygon(buffer, length);
            case BOX -> decodeBox(buffer, length);
            case CIRCLE -> decodeCircle(buffer, length);
            case LINE -> decodeLine(buffer, length);
            case LSEG -> decodeLineSegment(buffer, length);
            case PATH -> decodePath(buffer, length);

            case TSRANGE_OID -> decodeTimestampRange(buffer, length);
            case TSTZRANGE_OID -> decodeZonedTimestampRange(buffer, length);
            case DATERANGE_OID -> decodeDateRange(buffer, length);
            case INT4RANGE_OID -> decodeRange(buffer, length, (buf, len) -> buf.getInt(), Object::toString);
            case INT8RANGE_OID -> decodeRange(buffer, length, (buf, len) -> buf.getLong(), Object::toString);
            case NUM_RANGE_OID -> decodeRange(buffer, length, this::decodeNumeric, Object::toString);

            // These types don't have fixed OIDs
            default -> switch (type.getName()) {
                case "ltree" -> decodeLtree(buffer, length);
                case "hstore" -> decodeHstore(buffer, length);
                case "geometry", "geography" -> new PgBytes(decodeByteArray(buffer, length));
                case "citext" -> decodeStringOfLength(buffer, length);

                case "vector" -> decodePgVector(buffer, length);
                case "halfvec" -> decodePgHalfVector(buffer, length);
                case "sparsevec" -> decodePgSparseVector(buffer, length);

                case "tsvector" -> decodeTsVector(buffer, length);

                default -> {
                    if (includeUnknownDatatypes) {
                        yield decodeByteArray(buffer, length);
                    }
                    else {
                        buffer.position(buffer.position() + length);
                        yield null;
                    }
                }
            };
        };
    }

    /**
     * Decodes a tsvector from a ByteBuffer replication record.
     * <p>
     * Message format is:
     * <ol>
     *     <li>int4: lexeme count</li>
     *     <li>
     *         For that count repeat:
     *         <ol>
     *             <li>null-terminated string for the lexeme</li>
     *             <li>uint16: position count</li>
     *             <li>
     *                 for position count repeat:
     *                 uint16 represents a position: 2 bits for weight and remaining 14 for position value
     *             </li>
     *         </ol>
     *     </li>
     * </ol>
     *
     * @param buffer replication record buffer
     * @param length total length of the tsvector in bytes
     * @return PsTsVector object containing structured tsvector
     */
    private PgTsVector decodeTsVector(final ByteBuffer buffer, final int length) {
        final var lexemeCount = buffer.getInt();
        final var lexemes = new PgTsVector.Lexeme[lexemeCount];

        for (var i = 0; i < lexemeCount; i++) {
            final var lexeme = PgOutputMessageDecoder.readString(buffer);

            final var posCount = buffer.getShort() & 0xFFFF; // uint_16
            final var positions = new PgTsVector.Position[posCount];

            for (var p = 0; p < posCount; p++) {
                final var posData = buffer.getShort() & 0xFFFF; // uint_16
                final var weight = (posData >>> 14) & 0x3; // two bits
                final var position = posData & 0x3FFF; // remaining 14
                positions[p] = new PgTsVector.Position(position, PgTsVector.Weight.valueOf(weight));
            }

            lexemes[i] = new PgTsVector.Lexeme(lexeme, List.of(positions));
        }
        return new PgTsVector(List.of(lexemes));
    }

    private List<Double> decodePgVector(final ByteBuffer buffer, final int length) {
        final var dimensions = buffer.getShort();
        final var unused = buffer.getShort();
        final var values = new Double[dimensions];

        for (var i = 0; i < dimensions; i++) {
            values[i] = (double) buffer.getFloat();
        }

        return List.of(values);
    }

    private List<Float> decodePgHalfVector(final ByteBuffer buffer, final int length) {
        final var dimensions = buffer.getShort();
        final var unused = buffer.getShort();
        final var values = new Float[dimensions];

        for (var i = 0; i < dimensions; i++) {
            // should be replaced with Float.float16ToFloat when Debezium uses Java 21 baseline.
            values[i] = Float16.float16ToFloat(buffer.getShort());
        }

        return List.of(values);
    }

    private PgSparseVector decodePgSparseVector(final ByteBuffer buffer, final int length) {
        final var dimensions = buffer.getInt();
        final var nonzeroCount = buffer.getInt();
        final var unused = buffer.getInt();
        final var vector = new HashMap<Integer, Double>();

        final var nonzeroIndexes = new int[nonzeroCount];
        for (var i = 0; i < nonzeroCount; i++) {
            nonzeroIndexes[i] = buffer.getInt();
        }

        for (var i = 0; i < nonzeroCount; i++) {
            vector.put(nonzeroIndexes[i], (double) buffer.getFloat());
        }

        return new PgSparseVector(dimensions, vector);
    }

    private PgRange<OffsetDateTime> decodeZonedTimestampRange(final ByteBuffer buffer, final int length) {
        return decodeRange(buffer, length, this::decodeZonedTimestamp, value -> {
            if (PostgresValueConverter.POSITIVE_INFINITY_OFFSET_DATE_TIME.equals(value)) {
                return "infinity";
            }

            if (PostgresValueConverter.NEGATIVE_INFINITY_OFFSET_DATE_TIME.equals(value)) {
                return "-infinity";
            }

            return "\"%s\"".formatted(TIMESTAMP_TZ_FORMATTER.format(value));
        });
    }

    private PgRange<Instant> decodeTimestampRange(final ByteBuffer buffer, final int length) {
        return decodeRange(buffer, length, this::decodeTimestamp, value -> {
            if (PostgresValueConverter.POSITIVE_INFINITY_INSTANT.equals(value)) {
                return "infinity";
            }

            if (PostgresValueConverter.NEGATIVE_INFINITY_INSTANT.equals(value)) {
                return "-infinity";
            }

            return "\"%s\"".formatted(TIMESTAMP_FORMATTER.format(value));
        });
    }

    private PgRange<LocalDate> decodeDateRange(final ByteBuffer buffer, final int length) {
        return decodeRange(buffer, length, this::decodeDate, value -> {
            if (PostgresValueConverter.POSITIVE_INFINITY_LOCAL_DATE.equals(value)) {
                return "infinity";
            }

            if (PostgresValueConverter.NEGATIVE_INFINITY_LOCAL_DATE.equals(value)) {
                return "-infinity";
            }

            return DATE_FORMATTER.format(value);
        });
    }

    private <T> PgRange<T> decodeRange(final ByteBuffer buffer, final int length,
                                       final BiFunction<ByteBuffer, Integer, T> valueDecoder,
                                       final Function<T, String> valueStringifier) {
        final var flags = buffer.get();

        final var empty = (flags & RANGE_EMPTY) != 0;
        final var lowerInclusive = (flags & RANGE_LOWER_INC) != 0;
        final var upperInclusive = (flags & RANGE_UPPER_INC) != 0;
        final var lowerInfinite = (flags & RANGE_LOWER_INFINITY) != 0;
        final var upperInfinite = (flags & RANGE_UPPER_INFINITY) != 0;

        if (empty) {
            return new PgRange<>(null, null, true, false, false, false, false, valueStringifier);
        }

        final T lower;
        if (lowerInfinite) {
            lower = null;
        }
        else {
            lower = valueDecoder.apply(buffer, buffer.getInt());
        }

        final T upper;
        if (upperInfinite) {
            upper = null;
        }
        else {
            upper = valueDecoder.apply(buffer, buffer.getInt());
        }

        return new PgRange<>(lower, upper, false, lowerInclusive, upperInclusive, lowerInfinite, upperInfinite, valueStringifier);
    }

    private String decodeMacAddr(final ByteBuffer buffer, final int length, final int bytes) {
        final var sb = new StringBuilder();
        for (var i = 0; i < bytes; i++) {
            if (i > 0) {
                sb.append(":");
            }
            sb.append("%02x".formatted(buffer.get()));
        }
        return sb.toString();
    }

    private String decodeInet(final ByteBuffer buffer, final int length) {
        final var family = buffer.get();
        final var netmask = buffer.get();
        final var isCidr = buffer.get() == 1;
        final var addressLength = (int) buffer.get();

        final var bytes = new byte[addressLength];
        buffer.get(bytes);

        try {
            final var address = InetAddress.getByAddress(bytes).getHostAddress();

            if (netmask == 32) {
                return address;
            }
            else {
                return "%s/%d".formatted(address, netmask);
            }
        }
        catch (final UnknownHostException e) {
            throw new DebeziumException("Received bad Inet value", e);
        }
    }

    private BitSet decodeBit(final ByteBuffer buffer, final int length) {
        final var bitCount = buffer.getInt();
        final var byteCount = length - Integer.BYTES;
        final var bytes = new byte[byteCount];
        buffer.get(bytes);

        // Wrap the bytes in a BigInteger to the right in one operation. The bits sent by Postgres are right-padded,
        // while Debezium expect them to be left-padded.
        var bitBlock = new BigInteger(1, bytes);
        final var remainder = bitCount % 8;
        final var shiftAmount = (remainder == 0) ? 0 : (8 - remainder);
        if (shiftAmount > 0) {
            bitBlock = bitBlock.shiftRight(shiftAmount);
        }
        final var shifted = bitBlock.toByteArray();

        // BitSet expects the least significant bit to be on the left, unline BigInteger or Postgres sent bits. So it
        // must be reversed first.
        final var reversed = new byte[byteCount];
        for (var i = 0; i < byteCount; i++) {
            // BigInteger might add an extra 0x00 byte at the left of the array for the sign of the most significant but
            // of the input array is 1. So it's important to start taking bytes from the right of BigInteger returned
            // array so we drop that extra byte.
            reversed[i] = shifted[shifted.length - 1 - i];
        }

        return BitSet.valueOf(reversed).get(0, bitCount);
    }

    private PGbox decodeBox(final ByteBuffer buffer, final int length) {
        return new PGbox(
                buffer.getDouble(), // x1
                buffer.getDouble(), // y1
                buffer.getDouble(), // x2
                buffer.getDouble() // y2
        );
    }

    private PGcircle decodeCircle(final ByteBuffer buffer, final int length) {
        return new PGcircle(
                buffer.getDouble(), // x
                buffer.getDouble(), // y
                buffer.getDouble() // radius
        );
    }

    private PGline decodeLine(final ByteBuffer buffer, final int length) {
        return new PGline(
                buffer.getDouble(), // coefficient of x
                buffer.getDouble(), // coefficient of y
                buffer.getDouble() // constant
        );
    }

    private PGlseg decodeLineSegment(final ByteBuffer buffer, final int length) {
        return new PGlseg(
                buffer.getDouble(), // x1
                buffer.getDouble(), // y1
                buffer.getDouble(), // x2
                buffer.getDouble() // y2
        );
    }

    private PGpath decodePath(final ByteBuffer buffer, final int length) {
        final var closed = buffer.get() == 1;
        final var count = buffer.getInt();

        final var points = new PGpoint[count];

        for (var i = 0; i < count; i++) {
            points[i] = new PGpoint(buffer.getDouble(), buffer.getDouble());
        }

        return new PGpath(points, !closed);
    }

    private byte[] decodeByteArray(final ByteBuffer buffer, final int length) {
        final var bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    private PGpolygon decodePolygon(final ByteBuffer buffer, final int length) {
        final var count = buffer.getInt();
        final var points = new PGpoint[count];

        for (var i = 0; i < count; i++) {
            points[i] = new PGpoint(buffer.getDouble(), buffer.getDouble());
        }

        return new PGpolygon(points);
    }

    private PGpoint decodePoint(final ByteBuffer buffer, final int length) {
        return new PGpoint(
                buffer.getDouble(), // x
                buffer.getDouble() // y
        );
    }

    private PGInterval decodeInterval(final ByteBuffer buffer, final int length) {
        final var interval = new PGInterval();

        final var micros = buffer.getLong();
        final var days = buffer.getInt();
        final var months = buffer.getInt();

        final var hours = (int) (micros / MICROS_PER_HOUR);

        final var minutesMicros = micros % MICROS_PER_HOUR;
        final var minutes = (int) (minutesMicros / MICROS_PER_MINUTE);

        final var secondsMicros = micros % MICROS_PER_MINUTE;
        final var seconds = ((double) secondsMicros) / MICROS_PER_SECOND;

        interval.setValue(months / 12, months % 12, days, hours, minutes, seconds);

        return interval;
    }

    private BigDecimal decodeMoney(final ByteBuffer buffer, final int length, final TypeRegistry typeRegistry) {
        final var value = buffer.getLong();
        final var scale = typeRegistry.monetaryScale();
        final var divisionFactor = BigDecimal.valueOf(Math.pow(10, scale));
        return BigDecimal.valueOf(value).divide(divisionFactor, scale, RoundingMode.HALF_EVEN);
    }

    private Map<String, String> decodeHstore(final ByteBuffer buffer, final int length) {
        final var map = new HashMap<String, String>();
        final var count = buffer.getInt();

        for (var i = 0; i < count; i++) {
            final var key = decodeString(buffer);
            final var valueLength = buffer.getInt();
            final var value = valueLength < 0 ? null : decodeStringOfLength(buffer, valueLength);
            map.put(key, value);
        }

        return map;
    }

    private String decodeLtree(final ByteBuffer buffer, final int length) {
        if (buffer.get() != 1) {
            throw new DebeziumException("Invalid ltree version received for column");
        }
        return decodeStringOfLength(buffer, length - 1);
    }

    private Instant decodeTimestamp(final ByteBuffer buffer, final int length) {
        final var value = buffer.getLong();
        if (value == Long.MAX_VALUE) {
            return PostgresValueConverter.POSITIVE_INFINITY_INSTANT;
        }
        else if (value == Long.MIN_VALUE) {
            return PostgresValueConverter.NEGATIVE_INFINITY_INSTANT;
        }
        else {
            return PG_EPOCH_INSTANT.plus(value, ChronoUnit.MICROS);
        }
    }

    private OffsetDateTime decodeZonedTimestamp(final ByteBuffer buffer, final int length) {
        final var value = buffer.getLong();
        if (value == Long.MAX_VALUE) {
            return PostgresValueConverter.POSITIVE_INFINITY_OFFSET_DATE_TIME;
        }
        else if (value == Long.MIN_VALUE) {
            return PostgresValueConverter.NEGATIVE_INFINITY_OFFSET_DATE_TIME;
        }
        else {
            return PG_EPOCH_INSTANT.plus(value, ChronoUnit.MICROS)
                    .atOffset(ZoneOffset.UTC)
                    .withOffsetSameInstant(ZoneOffset.UTC);
        }
    }

    private LocalDate decodeDate(final ByteBuffer buffer, final int length) {
        final var value = buffer.getInt();

        if (value == Integer.MAX_VALUE) {
            return PostgresValueConverter.POSITIVE_INFINITY_LOCAL_DATE;
        }
        else if (value == Integer.MIN_VALUE) {
            return PostgresValueConverter.NEGATIVE_INFINITY_LOCAL_DATE;
        }
        else {
            return PG_EPOCH_DATE.plusDays(value);
        }
    }

    private Duration decodeTimeAsDuration(final ByteBuffer buffer, final int length) {
        final var value = buffer.getLong();
        return Duration.ofNanos(value * 1000);
    }

    private Object decodeZonedTime(final ByteBuffer buffer, final int length) {
        final var value = buffer.getLong();
        final var offset = -buffer.getInt();

        // Postgres has an end of the day marker `24:00:00Z`, while Java's OffsetTime is `23:59:59.999999999-18:00`
        if (value >= LocalTime.MAX.toNanoOfDay() / 1000) {
            return PostgresTimeBoundary.TIME_WITH_TIMEZONE_BOUNDARY_AT_UTC;
        }
        else {
            final var zoneOffset = ZoneOffset.ofTotalSeconds(offset);
            return OffsetTime.of(LocalTime.ofNanoOfDay(value * 1000), zoneOffset);
        }
    }

    private String decodeJsonb(final ByteBuffer buffer, final int length) {
        if (buffer.get() != 1) {
            throw new DebeziumException("Invalid JSONB version received for column");
        }
        return decodeStringOfLength(buffer, length - 1);
    }

    private Object decodeNumeric(final ByteBuffer buffer, final int length) {
        final var position = buffer.position();
        buffer.position(buffer.position() + length);
        final var value = ByteConverter.numeric(buffer.array(), buffer.arrayOffset() + position, length);

        if (value instanceof final Double dbl && dbl.isInfinite()) {
            return dbl < 0 ? SpecialValueDecimal.NEGATIVE_INF : SpecialValueDecimal.POSITIVE_INF;
        }
        else if (value instanceof final Double dbl && dbl.isNaN()) {
            return SpecialValueDecimal.NOT_A_NUMBER;
        }
        else {
            return value;
        }
    }

    /**
     * The implementation based on Postgres JDBC Driver {@link org.postgresql.jdbc.ArrayDecoding}
     */
    private Object[] decodeArray(final ByteBuffer buffer, final int length, final TypeRegistry typeRegistry,
                                 final boolean includeUnknownDatatypes) {
        final var dimensions = buffer.getInt();
        final var _ignored = buffer.getInt(); // null flags
        final var oid = buffer.getInt();

        if (dimensions == 0) {
            return new BigDecimal[0];
        }

        // Optimization for single dimension arrays
        if (dimensions == 1) {
            final var elementCount = buffer.getInt();
            final var __ignored = buffer.getInt();

            final var array = new Object[elementCount];

            for (var i = 0; i < elementCount; i++) {
                array[i] = decodeValue(buffer, typeRegistry, typeRegistry.get(oid), buffer.getInt(), includeUnknownDatatypes);
            }
            return array;
        }

        final var dimensionLengths = new int[dimensions];

        for (var i = 0; i < dimensions; i++) {
            dimensionLengths[i] = buffer.getInt();
            final var __ignored = buffer.getInt();
        }

        final var array = (Object[]) Array.newInstance(Object.class, dimensionLengths);

        decodeDimension(array, buffer, typeRegistry, typeRegistry.get(oid), dimensionLengths, 0, includeUnknownDatatypes);

        return array;
    }

    private void decodeDimension(final Object[] array, final ByteBuffer buffer, final TypeRegistry typeRegistry, final PostgresType type,
                                 final int[] dimensionLengths, final int dimension,
                                 final boolean includeUnknownDatatypes) {
        for (var i = 0; i < dimensionLengths[dimension]; i++) {
            if (dimension == dimensionLengths.length - 2) {
                // In Java multidimensional arrays are represented as array of arrays. We want to go deep enough in the dimensions until
                // we reach the one before last will represent the actual arrays that will hold the actual values.
                final var length = dimensionLengths[dimension + 1];
                for (var j = 0; j < length; j++) {
                    final var value = decodeValue(buffer, typeRegistry, type, buffer.getInt(), includeUnknownDatatypes);
                    ((Object[]) array[i])[j] = value;
                }
            }
            else {
                decodeDimension((Object[]) array[i], buffer, typeRegistry, type, dimensionLengths, dimension + 1, includeUnknownDatatypes);
            }
        }
    }

    private List<Object> arrayToList(final Object[] array) {
        // If the array is multidimensional, we will return a list of arrays.
        return Arrays.asList(array);
    }
}
