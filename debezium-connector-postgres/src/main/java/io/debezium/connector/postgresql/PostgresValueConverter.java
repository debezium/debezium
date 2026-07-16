/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static java.time.ZoneId.systemDefault;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.PGStatement;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGlseg;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.HStoreConverter;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.postgresql.PostgresConnectorConfig.HStoreHandlingMode;
import io.debezium.connector.postgresql.PostgresConnectorConfig.IntervalHandlingMode;
import io.debezium.connector.postgresql.connection.DateTimeFormat;
import io.debezium.connector.postgresql.data.Ltree;
import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.data.Bits;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.TsVector;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.geometry.Circle;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Line;
import io.debezium.data.geometry.Point;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.data.vector.SparseDoubleVector;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.spatial.WkbWriter;
import io.debezium.time.Conversions;
import io.debezium.time.Interval;
import io.debezium.time.MicroDuration;
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTime;
import io.debezium.time.StructuredZonedTimestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.NumberConversions;
import io.debezium.util.Strings;

/**
 * A provider of {@link ValueConverter}s and {@link SchemaBuilder}s for various Postgres specific column types.
 *
 * In addition to handling data type conversion from values coming from JDBC, this is also expected to handle data type
 * conversion for data types coming from the logical decoding plugin.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresValueConverter extends JdbcValueConverters {

    public static final Date POSITIVE_INFINITY_DATE = new Date(PGStatement.DATE_POSITIVE_INFINITY);
    public static final Timestamp POSITIVE_INFINITY_TIMESTAMP = new Timestamp(PGStatement.DATE_POSITIVE_INFINITY);
    public static final Instant POSITIVE_INFINITY_INSTANT = Conversions.toInstantFromMicros(PGStatement.DATE_POSITIVE_INFINITY);
    public static final LocalDateTime POSITIVE_INFINITY_LOCAL_DATE_TIME = LocalDateTime.ofInstant(POSITIVE_INFINITY_INSTANT, ZoneOffset.UTC);
    public static final OffsetDateTime POSITIVE_INFINITY_OFFSET_DATE_TIME = OffsetDateTime.ofInstant(Conversions.toInstantFromMillis(PGStatement.DATE_POSITIVE_INFINITY),
            ZoneOffset.UTC);
    public static final LocalDate POSITIVE_INFINITY_LOCAL_DATE = LocalDate.parse("-5877611-06-21");
    public static final String POSITIVE_INFINITY_TIMESTAMP_PG_STRING = "infinity";

    public static final Date NEGATIVE_INFINITY_DATE = new Date(PGStatement.DATE_NEGATIVE_INFINITY);
    public static final Timestamp NEGATIVE_INFINITY_TIMESTAMP = new Timestamp(PGStatement.DATE_NEGATIVE_INFINITY);
    public static final Instant NEGATIVE_INFINITY_INSTANT = Conversions.toInstantFromMicros(PGStatement.DATE_NEGATIVE_INFINITY);
    public static final LocalDateTime NEGATIVE_INFINITY_LOCAL_DATE_TIME = LocalDateTime.ofInstant(NEGATIVE_INFINITY_INSTANT, ZoneOffset.UTC);
    public static final OffsetDateTime NEGATIVE_INFINITY_OFFSET_DATE_TIME = OffsetDateTime.ofInstant(Conversions.toInstantFromMillis(PGStatement.DATE_NEGATIVE_INFINITY),
            ZoneOffset.UTC);
    public static final LocalDate NEGATIVE_INFINITY_LOCAL_DATE = LocalDate.parse("-5877611-06-22");
    public static final String NEGATIVE_INFINITY_TIMESTAMP_PG_STRING = "-infinity";

    /**
     * Variable scale decimal/numeric is defined by metadata
     * scale - 0
     * length - 131089
     */
    private static final int VARIABLE_SCALE_DECIMAL_LENGTH = 131089;

    /**
     * A string denoting not-a- number for FP and Numeric types
     */
    public static final String N_A_N = "NaN";

    /**
     * A string denoting positive infinity for FP and Numeric types
     */
    public static final String POSITIVE_INFINITY = "Infinity";

    /**
     * A string denoting negative infinity for FP and Numeric types
     */
    public static final String NEGATIVE_INFINITY = "-Infinity";

    private static final BigDecimal MICROSECONDS_PER_SECOND = new BigDecimal(1_000_000);

    /**
     * A formatter used to parse TIMETZ columns when provided as strings.
     */
    private static final DateTimeFormatter TIME_WITH_TIMEZONE_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .appendPattern("[XXX][XX][X]")
            .toFormatter();

    // Parses a PostgreSQL TIMETZ text value into raw components, allowing the end-of-day boundary hour 24
    // and preserving the original offset. Offset may be given as +HH, +HH:MM or +HH:MM:SS.
    private static final Pattern TIMETZ_PATTERN = Pattern.compile(
            "^(\\d{1,2}):(\\d{2}):(\\d{2})(?:\\.(\\d{1,6}))?([+-]\\d{2}(?::\\d{2}(?::\\d{2})?)?)$");

    /**
     * {@code true} if fields of data type not know should be handle as opaque binary;
     * {@code false} if they should be omitted
     */
    private final boolean includeUnknownDatatypes;

    private final TypeRegistry typeRegistry;
    private final HStoreHandlingMode hStoreMode;
    private final IntervalHandlingMode intervalMode;

    /**
     * The current database's character encoding.
     */
    private final Charset databaseCharset;

    private final JsonFactory jsonFactory;

    private final UnchangedToastedPlaceholder unchangedToastedPlaceholder;
    private final int moneyFractionDigits;

    public static PostgresValueConverter of(PostgresConnectorConfig connectorConfig, Charset databaseCharset, TypeRegistry typeRegistry) {
        return new PostgresValueConverter(
                databaseCharset,
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                null,
                connectorConfig.includeUnknownDatatypes(),
                typeRegistry,
                connectorConfig.hStoreHandlingMode(),
                connectorConfig.binaryHandlingMode(),
                connectorConfig.intervalHandlingMode(),
                new UnchangedToastedPlaceholder(connectorConfig),
                connectorConfig.moneyFractionDigits());
    }

    protected PostgresValueConverter(Charset databaseCharset, DecimalMode decimalMode,
                                     TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                                     BigIntUnsignedMode bigIntUnsignedMode, boolean includeUnknownDatatypes, TypeRegistry typeRegistry,
                                     HStoreHandlingMode hStoreMode, BinaryHandlingMode binaryMode, IntervalHandlingMode intervalMode,
                                     UnchangedToastedPlaceholder unchangedToastedPlaceholder, int moneyFractionDigits) {
        super(decimalMode, temporalPrecisionMode, defaultOffset, null, bigIntUnsignedMode, binaryMode);
        this.databaseCharset = databaseCharset;
        this.jsonFactory = new JsonFactory();
        this.includeUnknownDatatypes = includeUnknownDatatypes;
        this.typeRegistry = typeRegistry;
        this.hStoreMode = hStoreMode;
        this.intervalMode = intervalMode;
        this.moneyFractionDigits = moneyFractionDigits;
        this.unchangedToastedPlaceholder = unchangedToastedPlaceholder;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        int oidValue = column.nativeType();
        switch (oidValue) {
            case PgOid.BIT:
            case PgOid.BIT_ARRAY:
            case PgOid.VARBIT:
                return column.length() > 1 ? Bits.builder(column.length()) : SchemaBuilder.bool();
            case PgOid.INTERVAL:
                if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
                    return StructuredDuration.builder();
                }
                return intervalMode == IntervalHandlingMode.STRING ? Interval.builder() : MicroDuration.builder();
            case PgOid.TIMESTAMPTZ:
                // JDBC reports this as "timestamp" even though it's with tz, so we can't use the base class...
                if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
                    return StructuredZonedTimestamp.builder();
                }
                return ZonedTimestamp.builder();
            case PgOid.TIMETZ:
                // JDBC reports this as "time" but this contains TZ information
                if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
                    return StructuredZonedTime.builder();
                }
                return ZonedTime.builder();
            case PgOid.OID:
                return SchemaBuilder.int64();
            case PgOid.JSONB_OID:
            case PgOid.JSON:
                return Json.builder();
            case PgOid.TSRANGE_OID:
            case PgOid.TSTZRANGE_OID:
            case PgOid.DATERANGE_OID:
            case PgOid.INET_OID:
            case PgOid.CIDR_OID:
            case PgOid.MACADDR_OID:
            case PgOid.MACADDR8_OID:
            case PgOid.INT4RANGE_OID:
            case PgOid.NUM_RANGE_OID:
            case PgOid.INT8RANGE_OID:
            case PgOid.BPCHAR:
                return SchemaBuilder.string();
            case PgOid.UUID:
                return Uuid.builder();
            case PgOid.POINT:
                return Point.builder();
            case PgOid.BOX:
            case PgOid.LSEG:
            case PgOid.PATH:
            case PgOid.POLYGON:
                return Geometry.builder();
            case PgOid.CIRCLE:
                return Circle.builder();
            case PgOid.LINE:
                return Line.builder();
            case PgOid.MONEY:
                return moneySchema();
            case PgOid.NUMERIC:
                return numericSchema(column);
            case PgOid.BYTEA:
                return binaryMode.getSchema();
            case PgOid.TSVECTOR_OID:
                return TsVector.builder();
            case PgOid.INT2_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT16_SCHEMA);
            case PgOid.INT4_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA);
            case PgOid.INT8_ARRAY:
            case PgOid.OID_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT64_SCHEMA);
            case PgOid.CHAR_ARRAY:
            case PgOid.VARCHAR_ARRAY:
            case PgOid.TEXT_ARRAY:
            case PgOid.BPCHAR_ARRAY:
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
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
            case PgOid.NUMERIC_ARRAY:
                return SchemaBuilder.array(numericSchema(column).optional().build());
            case PgOid.FLOAT4_ARRAY:
                return SchemaBuilder.array(Schema.OPTIONAL_FLOAT32_SCHEMA);
            case PgOid.FLOAT8_ARRAY:
                return SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA);
            case PgOid.BOOL_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA);
            case PgOid.DATE_ARRAY:
                return SchemaBuilder.array(temporalPrecisionMode.getDateBuilder().optional().build());
            case PgOid.UUID_ARRAY:
                return SchemaBuilder.array(Uuid.builder().optional().build());
            case PgOid.JSONB_ARRAY:
            case PgOid.JSON_ARRAY:
                return SchemaBuilder.array(Json.builder().optional().build());
            case PgOid.TIME_ARRAY:
                return SchemaBuilder.array(temporalPrecisionMode.getTimeBuilder(getTimePrecision(column)).optional().build());
            case PgOid.TIMETZ_ARRAY:
                if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
                    return SchemaBuilder.array(StructuredZonedTime.builder().optional().build());
                }
                return SchemaBuilder.array(ZonedTime.builder().optional().build());
            case PgOid.TIMESTAMP_ARRAY:
                return SchemaBuilder.array(temporalPrecisionMode.getTimestampBuilder(getTimePrecision(column)).optional().build());
            case PgOid.TIMESTAMPTZ_ARRAY:
                if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
                    return SchemaBuilder.array(StructuredZonedTimestamp.builder().optional().build());
                }
                return SchemaBuilder.array(ZonedTimestamp.builder().optional().build());
            case PgOid.BYTEA_ARRAY:
                return SchemaBuilder.array(binaryMode.getSchema().optional().build());
            case PgOid.MONEY_ARRAY:
            case PgOid.NAME_ARRAY:
            case PgOid.INTERVAL_ARRAY:
            case PgOid.VARBIT_ARRAY:
            case PgOid.XML_ARRAY:
            case PgOid.POINT_ARRAY:
            case PgOid.REF_CURSOR_ARRAY:
                // These array types still need to be implemented. The superclass won't handle them so
                // we return null here until we can code schema implementations for them.
                return null;

            default:
                if (oidValue == typeRegistry.geometryOid()) {
                    return Geometry.builder();
                }
                else if (oidValue == typeRegistry.geographyOid()) {
                    return Geography.builder();
                }
                else if (oidValue == typeRegistry.citextOid()) {
                    return SchemaBuilder.string();
                }
                else if (oidValue == typeRegistry.geometryArrayOid()) {
                    return SchemaBuilder.array(Geometry.builder().optional().build());
                }
                else if (oidValue == typeRegistry.hstoreOid()) {
                    return hstoreSchema();
                }
                else if (oidValue == typeRegistry.ltreeOid()) {
                    return Ltree.builder();
                }
                else if (oidValue == typeRegistry.vectorOid()) {
                    return DoubleVector.builder();
                }
                else if (oidValue == typeRegistry.halfVectorOid()) {
                    return FloatVector.builder();
                }
                else if (oidValue == typeRegistry.sparseVectorOid()) {
                    return SparseDoubleVector.builder();
                }
                else if (oidValue == typeRegistry.hstoreArrayOid()) {
                    return SchemaBuilder.array(hstoreSchema().optional().build());
                }
                else if (oidValue == typeRegistry.geographyArrayOid()) {
                    return SchemaBuilder.array(Geography.builder().optional().build());
                }
                else if (oidValue == typeRegistry.citextArrayOid()) {
                    return SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
                }
                else if (oidValue == typeRegistry.ltreeArrayOid()) {
                    return SchemaBuilder.array(Ltree.builder().optional().build());
                }
                else if (oidValue == typeRegistry.isbn()) {
                    return SchemaBuilder.string();
                }

                final PostgresType resolvedType = typeRegistry.get(oidValue);
                if (resolvedType.isEnumType()) {
                    return io.debezium.data.Enum.builder(Strings.join(",", resolvedType.getEnumValues()));
                }
                else if (resolvedType.isArrayType()) {
                    if (resolvedType.getElementType().isEnumType()) {
                        Set<String> enumValues = resolvedType.getElementType().getEnumValues();
                        return SchemaBuilder.array(io.debezium.data.Enum.builder(Strings.join(",", enumValues)));
                    }
                    else {
                        // unfortunately, this does not work for array columns of domain types; the element type will have a
                        // non-matching JDBC id, resulting in no schema builder to be returned for those; the only way to export
                        // them right now is via 'includeUnknownDatatypes'
                        final SchemaBuilder jdbcSchemaBuilder = arrayElementSchema(column);

                        if (jdbcSchemaBuilder != null) {
                            return SchemaBuilder.array(jdbcSchemaBuilder);
                        }
                        else {
                            return includeUnknownDatatypes ? SchemaBuilder.array(binaryMode.getSchema()) : null;
                        }
                    }
                }
                else {
                    SchemaBuilder jdbcSchemaBuilder = super.schemaBuilder(column);

                    if (jdbcSchemaBuilder != null) {
                        return jdbcSchemaBuilder;
                    }
                    else {
                        return includeUnknownDatatypes ? binaryMode.getSchema() : null;
                    }
                }
        }
    }

    private SchemaBuilder numericSchema(Column column) {
        if (decimalMode == DecimalMode.PRECISE && isVariableScaleDecimal(column)) {
            return VariableScaleDecimal.builder();
        }
        return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().orElse(0));
    }

    private SchemaBuilder hstoreSchema() {
        if (hStoreMode == PostgresConnectorConfig.HStoreHandlingMode.JSON) {
            return Json.builder();
        }
        else {
            // keys are not nullable, but values are
            return SchemaBuilder.map(
                    SchemaBuilder.STRING_SCHEMA,
                    SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        }
    }

    private SchemaBuilder moneySchema() {
        switch (decimalMode) {
            case DOUBLE:
                return SchemaBuilder.float64();
            case PRECISE:
                return Decimal.builder(moneyFractionDigits);
            case STRING:
                return SchemaBuilder.string();
            default:
                throw new IllegalArgumentException("Unknown decimalMode");
        }
    }

    private SchemaBuilder arrayElementSchema(Column column) {
        PostgresType arrayType = typeRegistry.get(column.nativeType());
        PostgresType elementType = arrayType.getElementType();
        final String elementTypeName = elementType.getName();
        final String elementColumnName = column.name() + "-element";
        final Column elementColumn = Column.editor()
                .name(elementColumnName)
                .jdbcType(elementType.getJdbcId())
                .nativeType(elementType.getOid())
                .type(elementTypeName)
                .optional(true)
                .scale(column.scale().orElse(null))
                .length(column.length())
                .create();

        return schemaBuilder(elementColumn);
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        int oidValue = column.nativeType();
        switch (oidValue) {
            case PgOid.BIT:
            case PgOid.VARBIT:
                return convertBits(column, fieldDefn);
            case PgOid.INTERVAL:
                return data -> convertInterval(column, fieldDefn, data);
            case PgOid.TIME:
                return data -> convertTime(column, fieldDefn, data);
            case PgOid.TIMESTAMP:
                return ((ValueConverter) (data -> convertTimestampToLocalDateTime(column, fieldDefn, data))).and(super.converter(column, fieldDefn));
            case PgOid.TIMESTAMPTZ:
                return data -> convertTimestampWithZone(column, fieldDefn, data);
            case PgOid.TIMETZ:
                return data -> convertTimeWithZone(column, fieldDefn, data);
            case PgOid.OID:
                return data -> convertBigInt(column, fieldDefn, data);
            case PgOid.JSONB_OID:
            case PgOid.UUID:
            case PgOid.TSRANGE_OID:
            case PgOid.TSTZRANGE_OID:
            case PgOid.DATERANGE_OID:
            case PgOid.JSON:
            case PgOid.INET_OID:
            case PgOid.CIDR_OID:
            case PgOid.MACADDR_OID:
            case PgOid.MACADDR8_OID:
            case PgOid.INT4RANGE_OID:
            case PgOid.NUM_RANGE_OID:
            case PgOid.INT8RANGE_OID:
            case PgOid.TSVECTOR_OID:
            case PgOid.BPCHAR:
                return data -> convertString(column, fieldDefn, data);
            case PgOid.POINT:
                return data -> convertPoint(column, fieldDefn, data);
            case PgOid.BOX:
                return data -> convertBox(column, fieldDefn, data);
            case PgOid.LSEG:
                return data -> convertLseg(column, fieldDefn, data);
            case PgOid.PATH:
                return data -> convertPath(column, fieldDefn, data);
            case PgOid.POLYGON:
                return data -> convertPolygon(column, fieldDefn, data);
            case PgOid.CIRCLE:
                return data -> convertCircle(column, fieldDefn, data);
            case PgOid.LINE:
                return data -> convertLine(column, fieldDefn, data);
            case PgOid.MONEY:
                return data -> convertMoney(column, fieldDefn, data, decimalMode);
            case PgOid.NUMERIC:
                return (data) -> convertDecimal(column, fieldDefn, data, decimalMode);
            case PgOid.BYTEA:
                return data -> convertBinary(column, fieldDefn, data, binaryMode);
            case PgOid.INT2_ARRAY:
            case PgOid.INT4_ARRAY:
            case PgOid.INT8_ARRAY:
            case PgOid.CHAR_ARRAY:
            case PgOid.VARCHAR_ARRAY:
            case PgOid.TEXT_ARRAY:
            case PgOid.BPCHAR_ARRAY:
            case PgOid.NUMERIC_ARRAY:
            case PgOid.FLOAT4_ARRAY:
            case PgOid.FLOAT8_ARRAY:
            case PgOid.BOOL_ARRAY:
            case PgOid.DATE_ARRAY:
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
            case PgOid.UUID_ARRAY:
            case PgOid.JSONB_ARRAY:
            case PgOid.JSON_ARRAY:
            case PgOid.TIME_ARRAY:
            case PgOid.TIMETZ_ARRAY:
            case PgOid.TIMESTAMP_ARRAY:
            case PgOid.TIMESTAMPTZ_ARRAY:
            case PgOid.OID_ARRAY:
            case PgOid.BYTEA_ARRAY:
                return createArrayConverter(column, fieldDefn);

            // TODO DBZ-459 implement support for these array types; for now we just fall back to the default, i.e.
            // having no converter, so to be consistent with the schema definitions above
            case PgOid.MONEY_ARRAY:
            case PgOid.NAME_ARRAY:
            case PgOid.INTERVAL_ARRAY:
            case PgOid.VARBIT_ARRAY:
            case PgOid.XML_ARRAY:
            case PgOid.POINT_ARRAY:
            case PgOid.REF_CURSOR_ARRAY:
                return super.converter(column, fieldDefn);

            default:
                if (oidValue == typeRegistry.geometryOid()) {
                    return data -> convertGeometry(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.geographyOid()) {
                    return data -> convertGeography(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.citextOid()) {
                    return data -> convertCitext(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.hstoreOid()) {
                    return data -> convertHStore(column, fieldDefn, data, hStoreMode);
                }
                else if (oidValue == typeRegistry.ltreeOid()) {
                    return data -> convertLtree(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.vectorOid()) {
                    return data -> convertPgVector(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.halfVectorOid()) {
                    return data -> convertPgHalfVector(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.sparseVectorOid()) {
                    return data -> convertPgSparseVector(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.ltreeArrayOid()) {
                    return data -> convertLtreeArray(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.geometryArrayOid() ||
                        oidValue == typeRegistry.geographyArrayOid() ||
                        oidValue == typeRegistry.citextArrayOid() ||
                        oidValue == typeRegistry.hstoreArrayOid()) {
                    return createArrayConverter(column, fieldDefn);
                }
                else if (oidValue == typeRegistry.isbnOid()) {
                    return data -> convertIsbn(column, fieldDefn, data);
                }

                final PostgresType resolvedType = typeRegistry.get(oidValue);
                if (resolvedType.isArrayType()) {
                    return createArrayConverter(column, fieldDefn);
                }

                // Enum types don't have a JDBC converter, but we need to return a converter that passes through the string value
                if (resolvedType.isEnumType()) {
                    return data -> convertString(column, fieldDefn, data);
                }

                final ValueConverter jdbcConverter = super.converter(column, fieldDefn);
                if (jdbcConverter == null) {
                    return includeUnknownDatatypes ? data -> convertBinary(column, fieldDefn, data, binaryMode) : null;
                }
                else {
                    return jdbcConverter;
                }
        }
    }

    private ValueConverter createArrayConverter(Column column, Field fieldDefn) {
        PostgresType arrayType = typeRegistry.get(column.nativeType());
        PostgresType elementType = arrayType.getElementType();
        final String elementTypeName = elementType.getName();
        final String elementColumnName = column.name() + "-element";
        final Column elementColumn = Column.editor()
                .name(elementColumnName)
                .jdbcType(elementType.getJdbcId())
                .nativeType(elementType.getOid())
                .type(elementTypeName)
                .optional(true)
                .scale(column.scale().orElse(null))
                .length(column.length())
                .create();

        SchemaBuilder elementSchemaBuilder = schemaBuilder(elementColumn);
        if (elementSchemaBuilder == null) {
            return null;
        }
        Schema elementSchema = elementSchemaBuilder
                .optional()
                .build();

        final Field elementField = new Field(elementColumnName, 0, elementSchema);
        final ValueConverter elementConverter = converter(elementColumn, elementField);

        return data -> convertArray(column, fieldDefn, elementType, elementConverter, data);
    }

    @Override
    protected Object convertTime(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Strings.asDuration((String) data);
        }

        return super.convertTime(column, fieldDefn, data);
    }

    protected Object convertDecimal(Column column, Field fieldDefn, Object data, DecimalMode mode) {
        SpecialValueDecimal value;
        BigDecimal newDecimal;

        if (data instanceof SpecialValueDecimal) {
            value = (SpecialValueDecimal) data;

            if (value.getDecimalValue().isEmpty()) {
                return SpecialValueDecimal.fromLogical(value, mode, column.name());
            }
        }
        else {
            final Object o = toBigDecimal(column, fieldDefn, data);

            if (!(o instanceof BigDecimal)) {
                return o;
            }
            value = new SpecialValueDecimal((BigDecimal) o);
        }

        newDecimal = withScaleAdjustedIfNeeded(column, value.getDecimalValue().get());

        if (isVariableScaleDecimal(column) && mode == DecimalMode.PRECISE) {
            newDecimal = newDecimal.stripTrailingZeros();
            if (newDecimal.scale() < 0) {
                newDecimal = newDecimal.setScale(0);
            }

            return VariableScaleDecimal.fromLogical(fieldDefn.schema(), new SpecialValueDecimal(newDecimal));
        }

        return SpecialValueDecimal.fromLogical(new SpecialValueDecimal(newDecimal), mode, column.name());
    }

    @Override
    protected BigDecimal withScaleAdjustedIfNeeded(Column column, BigDecimal data) {
        BigDecimal value = super.withScaleAdjustedIfNeeded(column, data);
        // Deal with PostgreSQL where a default value will have higher scale than column
        if (column.scale().isPresent()) {
            // Check for Default DECIMAL
            if (column.length() != VARIABLE_SCALE_DECIMAL_LENGTH) {
                value = value.setScale(column.scale().get());
            }
        }
        return value;
    }

    protected Object convertHStore(Column column, Field fieldDefn, Object data, HStoreHandlingMode mode) {
        if (mode == HStoreHandlingMode.JSON) {
            return convertHstoreToJsonString(column, fieldDefn, data);
        }
        return convertHstoreToMap(column, fieldDefn, data);
    }

    private Object convertLtree(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", r -> {
            if (data instanceof byte[]) {
                r.deliver(new String((byte[]) data, databaseCharset));
            }
            if (data instanceof String) {
                r.deliver(data);
            }
            else if (data instanceof PGobject) {
                r.deliver(data.toString());
            }
        });
    }

    private Object convertPgVector(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, Collections.emptyList(), r -> {
            if (data instanceof byte[] typedData) {
                r.deliver(DoubleVector.fromLogical(fieldDefn.schema(), new String(typedData, databaseCharset)));
            }
            if (data instanceof String typedData) {
                r.deliver(DoubleVector.fromLogical(fieldDefn.schema(), typedData));
            }
            else if (data instanceof PGobject typedData) {
                r.deliver(DoubleVector.fromLogical(fieldDefn.schema(), typedData.getValue()));
            }
        });
    }

    private Object convertPgHalfVector(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, Collections.emptyList(), r -> {
            if (data instanceof byte[] typedData) {
                r.deliver(FloatVector.fromLogical(fieldDefn.schema(), new String(typedData, databaseCharset)));
            }
            if (data instanceof String typedData) {
                r.deliver(FloatVector.fromLogical(fieldDefn.schema(), typedData));
            }
            else if (data instanceof PGobject typedData) {
                r.deliver(FloatVector.fromLogical(fieldDefn.schema(), typedData.getValue()));
            }
        });
    }

    private Object convertPgSparseVector(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, Collections.emptyList(), r -> {
            if (data instanceof byte[] typedData) {
                r.deliver(SparseDoubleVector.fromLogical(fieldDefn.schema(), new String(typedData, databaseCharset)));
            }
            if (data instanceof String typedData) {
                r.deliver(SparseDoubleVector.fromLogical(fieldDefn.schema(), typedData));
            }
            else if (data instanceof PGobject typedData) {
                r.deliver(SparseDoubleVector.fromLogical(fieldDefn.schema(), typedData.getValue()));
            }
        });
    }

    private Object convertLtreeArray(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, Collections.emptyList(), r -> {
            if (data instanceof byte[]) {
                String s = new String((byte[]) data, databaseCharset);
                // remove '{' and '}'
                s = s.substring(1, s.length() - 1);
                List<String> ltrees = Arrays.asList(s.split(","));
                r.deliver(ltrees);
            }
            else if (data instanceof List) {
                List<Object> list = (List<Object>) data;
                List<String> ltrees = new ArrayList<>(list.size());
                for (Object value : list) {
                    ltrees.add(value.toString());
                }
                r.deliver(ltrees);
            }
            else if (data instanceof PgArray) {
                PgArray pgArray = (PgArray) data;
                try {
                    Object[] array = (Object[]) pgArray.getArray();
                    List<String> ltrees = new ArrayList<>(array.length);
                    for (Object value : array) {
                        ltrees.add(value.toString());
                    }
                    r.deliver(ltrees);
                }
                catch (SQLException e) {
                    logger.error("Failed to parse PgArray: " + pgArray, e);
                }
            }
        });
    }

    private Object convertHstoreToMap(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, Collections.emptyMap(), (r) -> {
            if (data instanceof String) {
                r.deliver(HStoreConverter.fromString((String) data));
            }
            else if (data instanceof byte[]) {
                r.deliver(HStoreConverter.fromString(asHstoreString((byte[]) data)));
            }
            else if (data instanceof PGobject) {
                r.deliver(HStoreConverter.fromString(data.toString()));
            }
            else if (data instanceof Map) {
                r.deliver(data);
            }
        });
    }

    /**
     * Returns an Hstore field as string in the form of {@code "key 1"=>"value1", "key_2"=>"val 1"}; i.e. the given byte
     * array is NOT the byte representation returned by {@link HStoreConverter#toBytes(Map,
     * org.postgresql.core.Encoding))}, but the String based representation
     */
    private String asHstoreString(byte[] data) {
        return new String(data, databaseCharset);
    }

    private Object convertHstoreToJsonString(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "{}", (r) -> {
            logger.trace("in ANON: value from data object: *** {} ***", data);
            logger.trace("in ANON: object type is: *** {} ***", data.getClass());
            if (data instanceof String) {
                r.deliver(changePlainStringRepresentationToJsonStringRepresentation(((String) data)));
            }
            else if (data instanceof byte[]) {
                r.deliver(changePlainStringRepresentationToJsonStringRepresentation(asHstoreString((byte[]) data)));
            }
            else if (data instanceof PGobject) {
                r.deliver(changePlainStringRepresentationToJsonStringRepresentation(data.toString()));
            }
            else if (data instanceof java.util.HashMap) {
                r.deliver(convertMapToJsonStringRepresentation((Map<String, String>) data));
            }
        });
    }

    private String changePlainStringRepresentationToJsonStringRepresentation(String text) {
        logger.trace("text value is: {}", text);
        try {
            Map<String, String> map = HStoreConverter.fromString(text);
            return convertMapToJsonStringRepresentation(map);
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't serialize hstore value into JSON: " + text, e);
        }
    }

    private String convertMapToJsonStringRepresentation(Map<String, String> map) {
        StringWriter writer = new StringWriter();
        try (JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer)) {
            jsonGenerator.writeStartObject();
            for (Entry<String, String> hstoreEntry : map.entrySet()) {
                jsonGenerator.writeStringField(hstoreEntry.getKey(), hstoreEntry.getValue());
            }
            jsonGenerator.writeEndObject();
            jsonGenerator.flush();
            return writer.getBuffer().toString();
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't serialize hstore value into JSON: " + map, e);
        }
    }

    @Override
    protected Object convertBit(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Integer.valueOf((String) data, 2);
        }
        return super.convertBit(column, fieldDefn, data);
    }

    @Override
    protected ValueConverter convertBits(Column column, Field fieldDefn) {
        // For VARBIT(1), we need special handling because JDBC returns PGobject
        if (column.nativeType() == PgOid.VARBIT && column.length() == 1) {
            return data -> {
                if (data instanceof PGobject pgObject) {
                    data = pgObject.getValue();
                }
                if (data instanceof String str) {
                    return Integer.valueOf(str, 2) == 0 ? Boolean.FALSE : Boolean.TRUE;
                }
                return convertBit(column, fieldDefn, data);
            };
        }
        return super.convertBits(column, fieldDefn);
    }

    @Override
    protected Object convertBits(Column column, Field fieldDefn, Object data, int numBytes) {
        if (data instanceof PGobject pgObject) {
            // returned by the JDBC driver
            data = pgObject.getValue();
        }

        // For VARBIT(1), convert to boolean just like BIT(1)
        if (column.length() == 1 && data instanceof String str) {
            // Return boolean directly
            return Integer.valueOf(str, 2) == 0 ? Boolean.FALSE : Boolean.TRUE;
        }

        if (data instanceof String) {
            String dataStr = (String) data;
            BitSet bitset = new BitSet(dataStr.length());
            int len = dataStr.length();
            for (int i = len - 1; i >= 0; i--) {
                if (dataStr.charAt(i) == '1') {
                    bitset.set(len - i - 1);
                }
            }
            /*
             * Instead of passing the default field precision/length value (= number of bits) divided by 8 = number of bytes
             * see {@link JdbcValueConverters#convertBits}, we re-calculate the length / number of bytes based on the actual
             * content. For example if we have only 3 bits set (b'101') but the field type is BIT VARYING (33) instead of
             * sending 5 bytes (b'10100000' plus 25 zero bits/4 zero bytes) we only pass one little-endian padded byte
             * (b'10100000').
             */
            int numBits = bitset.length();
            int bitBytes = numBits / Byte.SIZE + (numBits % Byte.SIZE == 0 ? 0 : 1);
            return super.convertBits(column, fieldDefn, bitset, bitBytes);
        }
        return super.convertBits(column, fieldDefn, data, numBytes);
    }

    protected Object convertMoney(Column column, Field fieldDefn, Object data, DecimalMode mode) {
        var fallback = decimalMode.equals(decimalMode.STRING) ? BigDecimal.ZERO.setScale(moneyFractionDigits).toString()
                : decimalMode.equals(decimalMode.DOUBLE) ? BigDecimal.ZERO.setScale(moneyFractionDigits).doubleValue() : BigDecimal.ZERO.setScale(moneyFractionDigits);
        return convertValue(column, fieldDefn, data, fallback, (r) -> {
            switch (mode) {
                case DOUBLE:
                    if (data instanceof BigDecimal) {
                        r.deliver(((BigDecimal) data).doubleValue());
                    }
                    else if (data instanceof Double) {
                        r.deliver(data);
                    }
                    else if (data instanceof Number) {
                        r.deliver(((Number) data).doubleValue());
                    }
                    break;
                case PRECISE:
                    if (data instanceof BigDecimal) {
                        r.deliver(((BigDecimal) data).setScale(moneyFractionDigits, RoundingMode.HALF_UP));
                    }
                    else if (data instanceof Double) {
                        r.deliver(BigDecimal.valueOf((Double) data).setScale(moneyFractionDigits, RoundingMode.HALF_UP));
                    }
                    else if (data instanceof Number) {
                        // the plugin will return a 64bit signed integer where the last #moneyFractionDigits are always decimals
                        r.deliver(BigDecimal.valueOf(((Number) data).longValue()).setScale(moneyFractionDigits, RoundingMode.HALF_UP));
                    }
                    break;
                case STRING:
                    if (data instanceof BigDecimal) {
                        r.deliver(((BigDecimal) data).toPlainString());
                    }
                    else {
                        r.deliver(String.valueOf(data));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown decimalMode");
            }
        });
    }

    protected Object convertInterval(Column column, Field fieldDefn, Object data) {
        if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
            return convertIntervalToStructured(column, fieldDefn, data);
        }
        Object fallback = intervalMode == IntervalHandlingMode.STRING ? Interval.toIsoString(0, 0, 0, 0, 0, new BigDecimal(0)) : NumberConversions.LONG_FALSE;
        return convertValue(column, fieldDefn, data, fallback, (r) -> {
            if (data instanceof Number) {
                final long micros = ((Number) data).longValue();
                if (intervalMode == IntervalHandlingMode.STRING) {
                    r.deliver(Interval.toIsoString(0, 0, 0, 0, 0, new BigDecimal(micros).divide(MICROSECONDS_PER_SECOND)));
                }
                else {
                    r.deliver(micros);
                }
            }
            if (data instanceof PGInterval) {
                final PGInterval interval = (PGInterval) data;
                if (intervalMode == IntervalHandlingMode.STRING) {
                    r.deliver(
                            Interval.toIsoString(
                                    interval.getYears(),
                                    interval.getMonths(),
                                    interval.getDays(),
                                    interval.getHours(),
                                    interval.getMinutes(),
                                    BigDecimal.valueOf(interval.getSeconds())));
                }
                else {
                    r.deliver(
                            MicroDuration.durationMicros(
                                    interval.getYears(),
                                    interval.getMonths(),
                                    interval.getDays(),
                                    interval.getHours(),
                                    interval.getMinutes(),
                                    interval.getSeconds(),
                                    MicroDuration.DAYS_PER_MONTH_AVG));
                }
            }
        });
    }

    protected Object convertIntervalToStructured(Column column, Field fieldDefn, Object data) {
        final int precision = getTimePrecision(column);
        return convertValue(column, fieldDefn, data, StructuredDuration.from(fieldDefn.schema(), 0, 0, 0, 0, 0, 0, 0, precision), (r) -> {
            if (data instanceof Number) {
                final long micros = ((Number) data).longValue();
                final long seconds = micros / 1_000_000;
                final int nanos = (int) (micros % 1_000_000) * 1_000;
                r.deliver(StructuredDuration.from(fieldDefn.schema(), 0, 0, 0, 0, 0, seconds, nanos, precision));
            }
            if (data instanceof PGInterval) {
                final PGInterval interval = (PGInterval) data;
                final BigDecimal seconds = BigDecimal.valueOf(interval.getSeconds());
                final long wholeSeconds = seconds.longValue();
                final int nanos = seconds.subtract(BigDecimal.valueOf(wholeSeconds))
                        .movePointRight(9)
                        .setScale(0, RoundingMode.HALF_UP)
                        .intValueExact();
                r.deliver(StructuredDuration.from(
                        fieldDefn.schema(),
                        interval.getYears(),
                        interval.getMonths(),
                        interval.getDays(),
                        interval.getHours(),
                        interval.getMinutes(),
                        wholeSeconds,
                        nanos,
                        precision));
            }
        });
    }

    @Override
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
            return convertPostgresTimestampWithZoneToStructured(column, fieldDefn, data);
        }
        if (data instanceof String str) {
            if (POSITIVE_INFINITY_TIMESTAMP_PG_STRING.equals(str) || NEGATIVE_INFINITY_TIMESTAMP_PG_STRING.equals(str)) {
                return str;
            }

            data = DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime(str).withOffsetSameInstant(ZoneOffset.UTC);
        }

        if (data instanceof java.util.Date) {
            // any Date like subclasses will be given to us by the JDBC driver, which uses the local VM TZ, so we need to go
            // back to GMT
            data = OffsetDateTime.ofInstant(((Date) data).toInstant(), ZoneOffset.UTC);
        }

        if (POSITIVE_INFINITY_OFFSET_DATE_TIME.equals(data)) {
            return POSITIVE_INFINITY_TIMESTAMP_PG_STRING;
        }
        else if (NEGATIVE_INFINITY_OFFSET_DATE_TIME.equals(data)) {
            return NEGATIVE_INFINITY_TIMESTAMP_PG_STRING;
        }
        else if (data instanceof OffsetDateTime) {
            data = ((OffsetDateTime) data).toZonedDateTime();
        }

        final Object javaData = data;
        return convertValue(column, fieldDefn, data, fallbackTimestampWithTimeZone, (r) -> {
            try {
                // Fractional width for zoned timestamp is set in scale if schema obtained via snapshot
                final Integer fraction = column.scale().orElse(column.length());
                r.deliver(ZonedTimestamp.toIsoString(javaData, defaultOffset, adjuster, fraction));
            }
            catch (IllegalArgumentException e) {
            }
        });
    }

    @Override
    protected Object convertTimeWithZone(Column column, Field fieldDefn, Object data) {
        // during snapshotting; already receiving OffsetTime @ UTC during streaming
        if (data instanceof String value) {
            // In STRUCTURED mode we preserve the raw clock components and the original offset (no UTC
            // normalization) and allow the PostgreSQL end-of-day boundary hour 24, which OffsetTime/LocalTime
            // cannot represent. The raw TIMETZ text is available both during snapshot and pgoutput streaming.
            if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
                return convertTimeWithZoneToStructuredPreservingOffset(column, fieldDefn, value);
            }
            if (PostgresTimeBoundary.isTimeWithTimeZoneBoundaryAtUtc(value)) {
                return PostgresTimeBoundary.TIME_WITH_TIMEZONE_BOUNDARY_AT_UTC;
            }

            // The TIMETZ column is returned as a String which we initially parse here
            // The parsed offset-time potentially has a zone-offset from the data, shift it after to GMT.
            final OffsetTime offsetTime = OffsetTime.parse(value, TIME_WITH_TIMEZONE_FORMATTER);
            data = offsetTime.withOffsetSameInstant(ZoneOffset.UTC);
        }

        return super.convertTimeWithZone(column, fieldDefn, data);
    }

    /**
     * STRUCTURED-mode TIMETZ conversion that preserves the raw hour (including the boundary 24), minute,
     * second, fractional seconds and the original offset from the PostgreSQL text value, without any UTC
     * normalization. This is the source-side half of full PG-to-PG TIMETZ fidelity (see debezium/dbz#2100).
     */
    private Object convertTimeWithZoneToStructuredPreservingOffset(Column column, Field fieldDefn, String data) {
        final int precision = getTimePrecision(column);
        final Schema schema = fieldDefn.schema();
        final Object fallback = StructuredZonedTime.from(schema, 0, 0, 0, 0, defaultOffset.getTotalSeconds(), precision);
        return convertValue(column, fieldDefn, data, fallback, (r) -> {
            try {
                final Matcher matcher = TIMETZ_PATTERN.matcher(data.trim());
                if (!matcher.matches()) {
                    logger.warn("Unexpected TIMETZ value for field {} with schema {}: value={}", fieldDefn.name(), schema, data);
                    return;
                }
                final int hour = Integer.parseInt(matcher.group(1));
                final int minute = Integer.parseInt(matcher.group(2));
                final int second = Integer.parseInt(matcher.group(3));
                final int nanos = parseFractionToNanos(matcher.group(4));
                final int offsetSeconds = parseOffsetSeconds(matcher.group(5));
                r.deliver(StructuredZonedTime.from(schema, hour, minute, second, nanos, offsetSeconds, precision));
            }
            catch (RuntimeException e) {
                logger.warn("Failed to convert TIMETZ value for field {} with schema {}: value={}", fieldDefn.name(), schema, data, e);
            }
        });
    }

    private static int parseFractionToNanos(String fraction) {
        if (fraction == null || fraction.isEmpty()) {
            return 0;
        }
        return Integer.parseInt((fraction + "000000000").substring(0, 9));
    }

    private static int parseOffsetSeconds(String offset) {
        final int sign = offset.charAt(0) == '-' ? -1 : 1;
        final String[] parts = offset.substring(1).split(":");
        int seconds = Integer.parseInt(parts[0]) * 3600;
        if (parts.length > 1) {
            seconds += Integer.parseInt(parts[1]) * 60;
        }
        if (parts.length > 2) {
            seconds += Integer.parseInt(parts[2]);
        }
        return sign * seconds;
    }

    protected Object convertPostgresTimestampWithZoneToStructured(Column column, Field fieldDefn, Object data) {
        if (isPositiveInfinityTimestampWithZone(data)) {
            return StructuredZonedTimestamp.positiveInfinity(fieldDefn.schema(), getTimePrecision(column));
        }
        if (isNegativeInfinityTimestampWithZone(data)) {
            return StructuredZonedTimestamp.negativeInfinity(fieldDefn.schema(), getTimePrecision(column));
        }
        if (data instanceof String str) {
            data = DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime(str).withOffsetSameInstant(ZoneOffset.UTC);
        }
        if (data instanceof java.util.Date) {
            data = OffsetDateTime.ofInstant(((Date) data).toInstant(), ZoneOffset.UTC);
        }
        if (data instanceof OffsetDateTime) {
            data = ((OffsetDateTime) data).toZonedDateTime();
        }
        return super.convertTimestampWithZone(column, fieldDefn, data);
    }

    @Override
    protected Object convertDateToStructured(Column column, Field fieldDefn, Object data) {
        if (isPositiveInfinityDate(data)) {
            return StructuredDate.positiveInfinity(fieldDefn.schema());
        }
        if (isNegativeInfinityDate(data)) {
            return StructuredDate.negativeInfinity(fieldDefn.schema());
        }
        return super.convertDateToStructured(column, fieldDefn, data);
    }

    @Override
    protected Object convertTimestampToStructured(Column column, Field fieldDefn, Object data) {
        if (isPositiveInfinityTimestamp(data)) {
            return StructuredTimestamp.positiveInfinity(fieldDefn.schema(), getTimePrecision(column));
        }
        if (isNegativeInfinityTimestamp(data)) {
            return StructuredTimestamp.negativeInfinity(fieldDefn.schema(), getTimePrecision(column));
        }
        return super.convertTimestampToStructured(column, fieldDefn, data);
    }

    private boolean isPositiveInfinityDate(Object data) {
        return POSITIVE_INFINITY_TIMESTAMP_PG_STRING.equals(data)
                || POSITIVE_INFINITY_DATE.equals(data)
                || POSITIVE_INFINITY_LOCAL_DATE.equals(data);
    }

    private boolean isNegativeInfinityDate(Object data) {
        return NEGATIVE_INFINITY_TIMESTAMP_PG_STRING.equals(data)
                || NEGATIVE_INFINITY_DATE.equals(data)
                || NEGATIVE_INFINITY_LOCAL_DATE.equals(data);
    }

    private boolean isPositiveInfinityTimestamp(Object data) {
        return POSITIVE_INFINITY_TIMESTAMP_PG_STRING.equals(data)
                || POSITIVE_INFINITY_TIMESTAMP.equals(data)
                || POSITIVE_INFINITY_LOCAL_DATE_TIME.equals(data)
                || POSITIVE_INFINITY_INSTANT.equals(data);
    }

    private boolean isNegativeInfinityTimestamp(Object data) {
        return NEGATIVE_INFINITY_TIMESTAMP_PG_STRING.equals(data)
                || NEGATIVE_INFINITY_TIMESTAMP.equals(data)
                || NEGATIVE_INFINITY_LOCAL_DATE_TIME.equals(data)
                || NEGATIVE_INFINITY_INSTANT.equals(data);
    }

    private boolean isPositiveInfinityTimestampWithZone(Object data) {
        return isPositiveInfinityTimestamp(data)
                || POSITIVE_INFINITY_DATE.equals(data)
                || POSITIVE_INFINITY_OFFSET_DATE_TIME.equals(data);
    }

    private boolean isNegativeInfinityTimestampWithZone(Object data) {
        return isNegativeInfinityTimestamp(data)
                || NEGATIVE_INFINITY_DATE.equals(data)
                || NEGATIVE_INFINITY_OFFSET_DATE_TIME.equals(data);
    }

    protected Object convertGeometry(Column column, Field fieldDefn, Object data) {
        final PostgisGeometry empty = PostgisGeometry.createEmpty();
        return convertValue(column, fieldDefn, data, io.debezium.data.geometry.Geometry.createValue(fieldDefn.schema(), empty.getWkb(), empty.getSrid()), (r) -> {
            try {
                final Schema schema = fieldDefn.schema();
                if (data instanceof byte[]) {
                    PostgisGeometry geom = PostgisGeometry.fromHexEwkb(new String((byte[]) data, StandardCharsets.US_ASCII));
                    r.deliver(io.debezium.data.geometry.Geometry.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
                else if (data instanceof PGobject) {
                    PGobject pgo = (PGobject) data;
                    PostgisGeometry geom = PostgisGeometry.fromHexEwkb(pgo.getValue());
                    r.deliver(io.debezium.data.geometry.Geometry.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
                else if (data instanceof String) {
                    PostgisGeometry geom = PostgisGeometry.fromHexEwkb((String) data);
                    r.deliver(io.debezium.data.geometry.Geometry.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
            }
            catch (IllegalArgumentException e) {
                logger.warn("Error converting to a Geometry type: {}", column);
            }
        });
    }

    protected Object convertGeography(Column column, Field fieldDefn, Object data) {
        final PostgisGeometry empty = PostgisGeometry.createEmpty();
        return convertValue(column, fieldDefn, data, io.debezium.data.geometry.Geography.createValue(fieldDefn.schema(), empty.getWkb(), empty.getSrid()), (r) -> {
            final Schema schema = fieldDefn.schema();
            try {
                if (data instanceof byte[]) {
                    PostgisGeometry geom = PostgisGeometry.fromHexEwkb(new String((byte[]) data, StandardCharsets.US_ASCII));
                    r.deliver(io.debezium.data.geometry.Geography.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
                else if (data instanceof PGobject) {
                    PGobject pgo = (PGobject) data;
                    PostgisGeometry geom = PostgisGeometry.fromHexEwkb(pgo.getValue());
                    r.deliver(io.debezium.data.geometry.Geography.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
                else if (data instanceof String) {
                    PostgisGeometry geom = PostgisGeometry.fromHexEwkb((String) data);
                    r.deliver(io.debezium.data.geometry.Geography.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
            }
            catch (IllegalArgumentException e) {
                logger.warn("Error converting to a Geography type: {}", column);
            }
        });
    }

    protected Object convertCitext(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            if (data instanceof byte[]) {
                r.deliver(new String((byte[]) data));
            }
            else if (data instanceof String) {
                r.deliver(data);
            }
            else if (data instanceof PGobject) {
                r.deliver(((PGobject) data).getValue());
            }
        });
    }

    protected Object convertIsbn(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            if (data instanceof byte[]) {
                r.deliver(new String((byte[]) data));
            }
            else if (data instanceof String) {
                r.deliver(data);
            }
            else if (data instanceof PGobject) {
                r.deliver(((PGobject) data).getValue());
            }
        });
    }

    /**
     * Converts a value representing a Postgres point for a column, to a Kafka Connect value.
     *
     * @param column the JDBC column; never null
     * @param fieldDefn the Connect field definition for this column; never null
     * @param data a data for the point column, either coming from the JDBC driver or logical decoding plugin
     * @return a value which will be used by Connect to represent the actual point value
     */
    protected Object convertPoint(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, Point.createValue(fieldDefn.schema(), 0, 0), (r) -> {
            final Schema schema = fieldDefn.schema();
            if (data instanceof PGpoint) {
                PGpoint pgPoint = (PGpoint) data;
                r.deliver(Point.createValue(schema, pgPoint.x, pgPoint.y));
            }
            else if (data instanceof String) {
                String dataString = data.toString();
                try {
                    PGpoint pgPoint = new PGpoint(dataString);
                    r.deliver(Point.createValue(schema, pgPoint.x, pgPoint.y));
                }
                catch (SQLException e) {
                    logger.warn("Error converting the string '{}' to a PGPoint type for the column '{}'", dataString, column);
                }
            }
            else if (data instanceof PgProto.Point) {
                r.deliver(Point.createValue(schema, ((PgProto.Point) data).getX(), ((PgProto.Point) data).getY()));
            }
        });
    }

    /**
     * Converts a PostgreSQL {@code box} to a {@link Geometry} value. The two opposing corners are
     * encoded as a coordinate-exact, closed 5-point polygon ring; the {@code box} label is preserved
     * in the extensions so the sink can reconstruct the native value.
     */
    protected Object convertBox(Column column, Field fieldDefn, Object data) {
        final Schema schema = fieldDefn.schema();
        return convertValue(column, fieldDefn, data, geometryFallback(schema, "box"), (r) -> {
            PGbox box = asGeometricValue(column, data, PGbox.class, PGbox::new);
            if (box != null) {
                double x1 = box.point[0].x, y1 = box.point[0].y;
                double x2 = box.point[1].x, y2 = box.point[1].y;
                byte[] wkb = WkbWriter.buildPolygon(List.of(List.of(
                        new double[]{ x1, y1 },
                        new double[]{ x1, y2 },
                        new double[]{ x2, y2 },
                        new double[]{ x2, y1 },
                        new double[]{ x1, y1 })));
                r.deliver(Geometry.createValue(schema, wkb, null, Map.of(Geometry.EXTENSION_TYPE_KEY, "box")));
            }
        });
    }

    /**
     * Converts a PostgreSQL {@code lseg} (a line segment between two points) to a {@link Geometry}
     * value encoded as a two-point line string.
     */
    protected Object convertLseg(Column column, Field fieldDefn, Object data) {
        final Schema schema = fieldDefn.schema();
        return convertValue(column, fieldDefn, data, geometryFallback(schema, "lseg"), (r) -> {
            // ReplicationMessage#asLseg is typed as Object, so accept any PGlseg the driver/decoder returns
            PGlseg lseg = asGeometricValue(column, data, PGlseg.class, PGlseg::new);
            if (lseg != null) {
                byte[] wkb = WkbWriter.buildLineString(List.of(
                        new double[]{ lseg.point[0].x, lseg.point[0].y },
                        new double[]{ lseg.point[1].x, lseg.point[1].y }));
                r.deliver(Geometry.createValue(schema, wkb, null, Map.of(Geometry.EXTENSION_TYPE_KEY, "lseg")));
            }
        });
    }

    /**
     * Converts a PostgreSQL {@code path} to a {@link Geometry} value encoded as a line string. A path
     * may be open or closed; that flag has no WKB representation, so it is preserved in the extensions.
     */
    protected Object convertPath(Column column, Field fieldDefn, Object data) {
        final Schema schema = fieldDefn.schema();
        return convertValue(column, fieldDefn, data, geometryFallback(schema, "path"), (r) -> {
            PGpath path = asGeometricValue(column, data, PGpath.class, PGpath::new);
            if (path != null) {
                List<double[]> points = new ArrayList<>(path.points.length);
                for (PGpoint point : path.points) {
                    points.add(new double[]{ point.x, point.y });
                }
                Map<String, String> extensions = new LinkedHashMap<>();
                extensions.put(Geometry.EXTENSION_TYPE_KEY, "path");
                extensions.put(Geometry.EXTENSION_CLOSED_KEY, String.valueOf(!path.open));
                r.deliver(Geometry.createValue(schema, WkbWriter.buildLineString(points), null, extensions));
            }
        });
    }

    /**
     * Converts a PostgreSQL {@code polygon} to a {@link Geometry} value encoded as a single closed
     * polygon ring.
     */
    protected Object convertPolygon(Column column, Field fieldDefn, Object data) {
        final Schema schema = fieldDefn.schema();
        return convertValue(column, fieldDefn, data, geometryFallback(schema, "polygon"), (r) -> {
            PGpolygon polygon = asGeometricValue(column, data, PGpolygon.class, PGpolygon::new);
            if (polygon != null) {
                List<double[]> ring = new ArrayList<>(polygon.points.length + 1);
                for (PGpoint point : polygon.points) {
                    ring.add(new double[]{ point.x, point.y });
                }
                closeRing(ring);
                byte[] wkb = WkbWriter.buildPolygon(List.of(ring));
                r.deliver(Geometry.createValue(schema, wkb, null, Map.of(Geometry.EXTENSION_TYPE_KEY, "polygon")));
            }
        });
    }

    /**
     * Converts a PostgreSQL {@code circle} to a {@link Circle} value carrying its true center and
     * radius. WKB has no curve primitive, so a circle cannot round-trip through {@link Geometry}.
     */
    protected Object convertCircle(Column column, Field fieldDefn, Object data) {
        final Schema schema = fieldDefn.schema();
        return convertValue(column, fieldDefn, data, Circle.createValue(schema, 0, 0, 0), (r) -> {
            PGcircle circle = asGeometricValue(column, data, PGcircle.class, PGcircle::new);
            if (circle != null) {
                r.deliver(Circle.createValue(schema, circle.center.x, circle.center.y, circle.radius));
            }
        });
    }

    /**
     * Converts a PostgreSQL {@code line} (an infinite line {@code Ax + By + C = 0}) to a {@link Line}
     * value carrying its three coefficients.
     */
    protected Object convertLine(Column column, Field fieldDefn, Object data) {
        final Schema schema = fieldDefn.schema();
        // PostgreSQL rejects a line whose A and B coefficients are both zero, so the NOT NULL fallback
        // uses {0,1,0} (the y = 0 axis) rather than the all-zero triple.
        return convertValue(column, fieldDefn, data, Line.createValue(schema, 0, 1, 0), (r) -> {
            PGline line = asGeometricValue(column, data, PGline.class, PGline::new);
            if (line != null) {
                r.deliver(Line.createValue(schema, line.a, line.b, line.c));
            }
        });
    }

    /**
     * Coerces incoming column data to the requested {@code org.postgresql.geometric} type. Logical
     * decoding delivers the {@code PGxxx} object directly, while snapshots via the JDBC driver may
     * hand back the canonical text, which the given factory parses. Returns {@code null} (leaving the
     * value unconverted) when the type is unexpected or the text cannot be parsed.
     */
    private <T> T asGeometricValue(Column column, Object data, Class<T> type, PGobjectFactory<T> factory) {
        if (type.isInstance(data)) {
            return type.cast(data);
        }
        if (data instanceof String) {
            String dataString = data.toString();
            try {
                return factory.parse(dataString);
            }
            catch (SQLException e) {
                logger.warn("Error converting the string '{}' to a {} for the column '{}'", dataString, type.getSimpleName(), column);
            }
        }
        return null;
    }

    /**
     * Builds the non-null fallback a NOT NULL geometric column falls back to when a null value is
     * received with no column default (see {@link #convertValue}). When {@code column.propagate.source.type}
     * is unset the sink binds these through PostGIS {@code ST_GeomFromWKB}, which enforces the OGC
     * minimum-vertex rules (a ring needs at least four points, a line string at least two) and rejects
     * degenerate geometry. So each fallback is a small but genuinely valid shape rather than a repeated
     * origin point: a {@code box}/{@code polygon} is a unit square/triangle ring (the box is rebuilt from
     * ring vertices 0 and 2, which the square places on opposite corners), and an {@code lseg}/{@code path}
     * is a two distinct-point segment.
     */
    // package-private for direct unit testing of the OGC minimum-vertex invariant
    static Struct geometryFallback(Schema schema, String type) {
        final byte[] wkb;
        switch (type) {
            case "box":
                wkb = WkbWriter.buildPolygon(List.of(List.of(
                        new double[]{ 0, 0 }, new double[]{ 0, 1 }, new double[]{ 1, 1 },
                        new double[]{ 1, 0 }, new double[]{ 0, 0 })));
                break;
            case "lseg":
                wkb = WkbWriter.buildLineString(List.of(new double[]{ 0, 0 }, new double[]{ 1, 0 }));
                break;
            case "polygon":
                wkb = WkbWriter.buildPolygon(List.of(List.of(
                        new double[]{ 0, 0 }, new double[]{ 1, 0 }, new double[]{ 0, 1 }, new double[]{ 0, 0 })));
                break;
            default: // path
                wkb = WkbWriter.buildLineString(List.of(new double[]{ 0, 0 }, new double[]{ 1, 0 }));
                break;
        }
        return Geometry.createValue(schema, wkb, null, Map.of(Geometry.EXTENSION_TYPE_KEY, type));
    }

    private static void closeRing(List<double[]> ring) {
        if (ring.size() >= 2) {
            double[] first = ring.get(0);
            double[] last = ring.get(ring.size() - 1);
            if (first[0] != last[0] || first[1] != last[1]) {
                ring.add(new double[]{ first[0], first[1] });
            }
        }
    }

    /**
     * Factory that parses a {@code org.postgresql.geometric} value from its canonical text form.
     */
    @FunctionalInterface
    private interface PGobjectFactory<T> {
        T parse(String value) throws SQLException;
    }

    protected Object convertArray(Column column, Field fieldDefn, PostgresType elementType, ValueConverter elementConverter, Object data) {
        return convertValue(column, fieldDefn, data, Collections.emptyList(), (r) -> {
            if (data instanceof List) {
                r.deliver(((List<?>) data).stream()
                        .map(value -> resolveArrayValue(value, elementType))
                        .map(elementConverter::convert)
                        .collect(Collectors.toList()));
            }
            else if (data instanceof PgArray array) {
                try {
                    final List<Object> converted;
                    if (elementType.getOid() == PgOid.TIMETZ) {
                        converted = convertTimeWithTimeZoneArray(array, elementType, elementConverter);
                    }
                    else {
                        final Object[] values = (Object[]) array.getArray();
                        converted = new ArrayList<>(values.length);
                        for (Object value : values) {
                            converted.add(elementConverter.convert(resolveArrayValue(value, elementType)));
                        }
                    }
                    r.deliver(converted);
                }
                catch (SQLException e) {
                    throw new ConnectException("Failed to read value of array " + column.name());
                }
            }
        });
    }

    private List<Object> convertTimeWithTimeZoneArray(PgArray data, PostgresType elementType, ValueConverter elementConverter) throws SQLException {
        final List<Object> converted = new ArrayList<>();
        try (ResultSet values = data.getResultSet()) {
            while (values.next()) {
                converted.add(elementConverter.convert(resolveArrayValue(values.getString(2), elementType)));
            }
        }
        return converted;
    }

    private Object resolveArrayValue(Object value, PostgresType elementType) {
        // PostgreSQL time data types with time-zones are handled differently when included in an array.
        // The values are automatically translated to the local JVM time-zone and need to be converted back to GMT
        // before delegating the value to the element converter.
        switch (elementType.getOid()) {
            case PgOid.TIMETZ: {
                if (value instanceof java.sql.Time) {
                    ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDate.now(), ((java.sql.Time) value).toLocalTime(), systemDefault());
                    // Daylight savings gets applied by PgArray, need to account for that here
                    zonedDateTime = zonedDateTime.plus(systemDefault().getRules().getDaylightSavings(zonedDateTime.toInstant()));
                    return OffsetTime.of(zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalTime(), ZoneOffset.UTC);
                }
                break;
            }
            case PgOid.TIMESTAMPTZ: {
                if (value instanceof java.sql.Timestamp) {
                    // Daylight savings isn't applied here, no need to account for it
                    ZonedDateTime zonedDateTime = ZonedDateTime.of(((java.sql.Timestamp) value).toLocalDateTime(), systemDefault());
                    return OffsetDateTime.of(zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime(), ZoneOffset.UTC);
                }
            }
        }

        // no special handling is needed, return value as-is
        return value;
    }

    private boolean isVariableScaleDecimal(final Column column) {
        // TODO: Remove VARIABLE_SCALE_DECIMAL_LENGTH when https://github.com/pgjdbc/pgjdbc/issues/2275
        // is closed.
        return (column.length() == 0 || column.length() == VARIABLE_SCALE_DECIMAL_LENGTH) &&
                column.scale().orElseGet(() -> 0) == 0;
    }

    public static Optional<SpecialValueDecimal> toSpecialValue(String value) {
        switch (value) {
            case N_A_N:
                return Optional.of(SpecialValueDecimal.NOT_A_NUMBER);
            case POSITIVE_INFINITY:
                return Optional.of(SpecialValueDecimal.POSITIVE_INF);
            case NEGATIVE_INFINITY:
                return Optional.of(SpecialValueDecimal.NEGATIVE_INF);
        }

        return Optional.empty();
    }

    protected Object convertTimestampToLocalDateTime(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof String s) {
            return switch (s) {
                case POSITIVE_INFINITY_TIMESTAMP_PG_STRING -> POSITIVE_INFINITY_LOCAL_DATE_TIME;
                case NEGATIVE_INFINITY_TIMESTAMP_PG_STRING -> NEGATIVE_INFINITY_LOCAL_DATE_TIME;
                default -> {
                    final Instant instant = DateTimeFormat.get().timestampToInstant(s);
                    yield LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                }
            };
        }
        if (!(data instanceof Timestamp)) {
            return data;
        }
        final Timestamp timestamp = (Timestamp) data;

        if (POSITIVE_INFINITY_TIMESTAMP.equals(timestamp)) {
            return POSITIVE_INFINITY_LOCAL_DATE_TIME;
        }
        else if (NEGATIVE_INFINITY_TIMESTAMP.equals(timestamp)) {
            return NEGATIVE_INFINITY_LOCAL_DATE_TIME;
        }

        final Instant instant = timestamp.toInstant();

        return LocalDateTime.ofInstant(instant, ZoneOffset.systemDefault());
    }

    @Override
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            return null;
        }

        if (data instanceof Instant instant) {
            if (POSITIVE_INFINITY_INSTANT.equals(instant)) {
                return Long.MAX_VALUE;
            }
            else if (NEGATIVE_INFINITY_INSTANT.equals(instant)) {
                return Long.MIN_VALUE;
            }
        }

        if (data instanceof LocalDateTime localDateTime) {
            if (POSITIVE_INFINITY_LOCAL_DATE_TIME.equals(localDateTime)) {
                return Long.MAX_VALUE;
            }
            else if (NEGATIVE_INFINITY_LOCAL_DATE_TIME.equals(localDateTime)) {
                return Long.MIN_VALUE;
            }
        }

        return super.convertTimestampToEpochNanos(column, fieldDefn, data);
    }

    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().orElse(-1);
    }

    /**
     * Extracts a value from a PGobject .
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a Kafka Connect type
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    @Override
    protected Object convertBinaryToBytes(Column column, Field fieldDefn, Object data) {
        if (data == UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE) {
            return unchangedToastedPlaceholder.getToastPlaceholderBinary();
        }
        if (data instanceof PgArray) {
            data = ((PgArray) data).toString();
        }
        return super.convertBinaryToBytes(column, fieldDefn, (data instanceof PGobject) ? ((PGobject) data).getValue() : data);
    }

    @Override
    protected Object convertBinaryToBase64(Column column, Field fieldDefn, Object data) {
        return super.convertBinaryToBase64(column, fieldDefn, (data instanceof PGobject) ? ((PGobject) data).getValue() : data);
    }

    @Override
    protected Object convertBinaryToBase64UrlSafe(Column column, Field fieldDefn, Object data) {
        return super.convertBinaryToBase64UrlSafe(column, fieldDefn, (data instanceof PGobject) ? ((PGobject) data).getValue() : data);
    }

    @Override
    protected Object convertBinaryToHex(Column column, Field fieldDefn, Object data) {
        if (data == UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE) {
            return unchangedToastedPlaceholder.getToastPlaceholderString();
        }
        return super.convertBinaryToHex(column, fieldDefn, (data instanceof PGobject) ? ((PGobject) data).getValue() : data);
    }

    /**
     * Replaces toasted value with a placeholder
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a Kafka Connect type
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data == UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE) {
            return unchangedToastedPlaceholder.getToastPlaceholderString();
        }
        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object handleUnknownData(Column column, Field fieldDefn, Object data) {
        Optional<Object> toastedArrayPlaceholder = unchangedToastedPlaceholder.getValue(data);
        if (toastedArrayPlaceholder.isPresent()) {
            if (UnchangedToastedReplicationMessageColumn.UNCHANGED_HSTORE_TOAST_VALUE == data) {
                if (HStoreHandlingMode.JSON.equals(hStoreMode)) {
                    return convertMapToJsonStringRepresentation((Map<String, String>) toastedArrayPlaceholder.get());
                }
            }
            return toastedArrayPlaceholder.get();
        }
        return super.handleUnknownData(column, fieldDefn, data);
    }
}
