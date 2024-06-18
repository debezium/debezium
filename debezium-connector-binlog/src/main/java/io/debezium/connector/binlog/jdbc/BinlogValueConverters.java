/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.jdbc;

import static io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode.FAIL;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogGeometry;
import io.debezium.connector.binlog.BinlogUnsignedIntegerConverter;
import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.ValueConverter;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.time.Year;
import io.debezium.util.Loggings;
import io.debezium.util.Strings;

/**
 * Binlog-based connector-specific customizations for converting JDBC values obtained from the binlog client
 * library.<p></p>
 *
 * This class uses UTC for the default time zone when converting values without timezone details to values that
 * require timezones. This is because MariaDB {@code TIMESTAMP} values are always stored in UTC, unlike types
 * like {@code DATETIME}, and aare replicated as such. Meanwhile, the Binlog Client will deserialize these as
 * {@link java.sql.Timestamp} which have no timezone; therefore, are presumed to be UTC.<p></p>
 *
 * If a column is {@link java.sql.Types#TIMESTAMP_WITH_TIMEZONE}, the converters will need to convert the value
 * from a {@link java.sql.Timestamp} to an {@link java.time.OffsetDateTime} using the default time zone, which
 * is always UTC.
 *
 * @author Randall Hauch
 * @author Chris Cranford
 * @see com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer
 */
@Immutable
public abstract class BinlogValueConverters extends JdbcValueConverters {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogValueConverters.class);
    private static final Logger INVALID_VALUE_LOGGER = LoggerFactory.getLogger(BinlogValueConverters.class.getName() + ".invalid_value");

    /**
     * Used to parse values of TIME columns. Format: 000:00:00.000000.
     */
    private static final Pattern TIME_FIELD_PATTERN = Pattern.compile("(\\-?[0-9]*):([0-9]*)(:([0-9]*))?(\\.([0-9]*))?");

    /**
     * Used to parse values of DATE columns. Format: 000-00-00.
     */
    private static final Pattern DATE_FIELD_PATTERN = Pattern.compile("([0-9]*)-([0-9]*)-([0-9]*)");

    /**
     * Used to parse values of TIMESTAMP columns. Format: 000-00-00 00:00:00.000.
     */
    private static final Pattern TIMESTAMP_FIELD_PATTERN = Pattern.compile("([0-9]*)-([0-9]*)-([0-9]*) .*");

    private final EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode;
    private final BinlogCharsetRegistry charsetRegistry;

    /**
     * Create a new instance of the value converters that always uses UTC for the default time zone when
     * converting values without timezone information to values that require timezones.
     *
     * @param decimalMode how {@code DECIMAL} and {@code NUMERIC} values are treated; can be null if {@link DecimalMode#PRECISE} is used
     * @param temporalPrecisionMode temporal precision mode
     * @param bigIntUnsignedMode how {@code BIGINT UNSIGNED} values are treated; may be null if {@link BigIntUnsignedMode#PRECISE} is used.
     * @param binaryHandlingMode how binary columns should be treated
     * @param adjuster a temporal adjuster to make a database specific time before conversion
     * @param eventConvertingFailureHandlingMode how to handle conversion failures
     * @param serviceRegistry the service registry instance, should not be {@code null}
     */
    public BinlogValueConverters(DecimalMode decimalMode,
                                 TemporalPrecisionMode temporalPrecisionMode,
                                 BigIntUnsignedMode bigIntUnsignedMode,
                                 BinaryHandlingMode binaryHandlingMode,
                                 TemporalAdjuster adjuster,
                                 EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode,
                                 ServiceRegistry serviceRegistry) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, adjuster, bigIntUnsignedMode, binaryHandlingMode);
        this.eventConvertingFailureHandlingMode = eventConvertingFailureHandlingMode;
        this.charsetRegistry = serviceRegistry.getService(BinlogCharsetRegistry.class);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        // Handle database-specific types based upon how they are handled by the binlog client
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "JSON")) {
            return Json.builder();
        }
        if (matches(typeName, "POINT")) {
            return io.debezium.data.geometry.Point.builder();
        }
        if (matches(typeName, "GEOMETRY")
                || matches(typeName, "LINESTRING")
                || matches(typeName, "POLYGON")
                || matches(typeName, "MULTIPOINT")
                || matches(typeName, "MULTILINESTRING")
                || matches(typeName, "MULTIPOLYGON")
                || isGeometryCollection(typeName)) {
            return io.debezium.data.geometry.Geometry.builder();
        }
        if (matches(typeName, "YEAR")) {
            return Year.builder();
        }
        if (matches(typeName, "ENUM")) {
            String commaSeparatedOptions = extractEnumAndSetOptionsAsString(column);
            return io.debezium.data.Enum.builder(commaSeparatedOptions);
        }
        if (matches(typeName, "SET")) {
            String commaSeparatedOptions = extractEnumAndSetOptionsAsString(column);
            return io.debezium.data.EnumSet.builder(commaSeparatedOptions);
        }
        if (matches(typeName, "SMALLINT UNSIGNED") || matches(typeName, "SMALLINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT2 UNSIGNED") || matches(typeName, "INT2 UNSIGNED ZEROFILL")) {
            // In order to capture unsigned SMALLINT 16-bit data source, INT32 will be required to safely capture all valid values
            // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
            return SchemaBuilder.int32();
        }
        if (matches(typeName, "INT UNSIGNED") || matches(typeName, "INT UNSIGNED ZEROFILL")
                || matches(typeName, "INT4 UNSIGNED") || matches(typeName, "INT4 UNSIGNED ZEROFILL")) {
            // In order to capture unsigned INT 32-bit data source, INT64 will be required to safely capture all valid values
            // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
            return SchemaBuilder.int64();
        }
        if (matches(typeName, "BIGINT UNSIGNED") || matches(typeName, "BIGINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT8 UNSIGNED") || matches(typeName, "INT8 UNSIGNED ZEROFILL")) {
            switch (super.bigIntUnsignedMode) {
                case LONG:
                    return SchemaBuilder.int64();
                case PRECISE:
                    // In order to capture unsigned INT 64-bit data source, org.apache.kafka.connect.data.Decimal:Byte will be required to safely capture all valid values with scale of 0
                    // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
                    return SpecialValueDecimal.builder(DecimalMode.PRECISE, 20, 0);
            }
        }
        if ((matches(typeName, "FLOAT")
                || matches(typeName, "FLOAT UNSIGNED")
                || matches(typeName, "FLOAT UNSIGNED ZEROFILL"))
                && column.scale().isEmpty() && column.length() <= 24) {
            return SchemaBuilder.float32();
        }
        // Otherwise, let the base class handle it ...
        return super.schemaBuilder(column);
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        // Handle a few database-specific types based upon how they are handled by the binlog client
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "JSON")) {
            return (data) -> convertJson(column, fieldDefn, data);
        }
        if (matches(typeName, "GEOMETRY")
                || matches(typeName, "LINESTRING")
                || matches(typeName, "POLYGON")
                || matches(typeName, "MULTIPOINT")
                || matches(typeName, "MULTILINESTRING")
                || matches(typeName, "MULTIPOLYGON")
                || isGeometryCollection(typeName)) {
            return (data -> convertGeometry(column, fieldDefn, data));
        }
        if (matches(typeName, "POINT")) {
            // backwards compatibility
            return (data -> convertPoint(column, fieldDefn, data));
        }
        if (matches(typeName, "YEAR")) {
            return (data) -> convertYearToInt(column, fieldDefn, data);
        }
        if (matches(typeName, "ENUM")) {
            // Build up the character array based upon the column's type ...
            List<String> options = extractEnumAndSetOptions(column);
            return (data) -> convertEnumToString(options, column, fieldDefn, data);
        }
        if (matches(typeName, "SET")) {
            // Build up the character array based upon the column's type ...
            List<String> options = extractEnumAndSetOptions(column);
            return (data) -> convertSetToString(options, column, fieldDefn, data);
        }
        if (matches(typeName, "TINYINT UNSIGNED") || matches(typeName, "TINYINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT1 UNSIGNED") || matches(typeName, "INT1 UNSIGNED ZEROFILL")) {
            // Convert TINYINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
            return (data) -> convertUnsignedTinyint(column, fieldDefn, data);
        }
        if (matches(typeName, "SMALLINT UNSIGNED") || matches(typeName, "SMALLINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT2 UNSIGNED") || matches(typeName, "INT2 UNSIGNED ZEROFILL")) {
            // Convert SMALLINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
            return (data) -> convertUnsignedSmallint(column, fieldDefn, data);
        }
        if (matches(typeName, "MEDIUMINT UNSIGNED") || matches(typeName, "MEDIUMINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT3 UNSIGNED") || matches(typeName, "INT3 UNSIGNED ZEROFILL")
                || matches(typeName, "MIDDLEINT UNSIGNED") || matches(typeName, "MIDDLEINT UNSIGNED ZEROFILL")) {
            // Convert MEDIUMINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
            return (data) -> convertUnsignedMediumint(column, fieldDefn, data);
        }
        if (matches(typeName, "INT UNSIGNED") || matches(typeName, "INT UNSIGNED ZEROFILL")
                || matches(typeName, "INT4 UNSIGNED") || matches(typeName, "INT4 UNSIGNED ZEROFILL")) {
            // Convert INT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
            return (data) -> convertUnsignedInt(column, fieldDefn, data);
        }
        if (matches(typeName, "BIGINT UNSIGNED") || matches(typeName, "BIGINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT8 UNSIGNED") || matches(typeName, "INT8 UNSIGNED ZEROFILL")) {
            switch (super.bigIntUnsignedMode) {
                case LONG:
                    return (data) -> convertBigInt(column, fieldDefn, data);
                case PRECISE:
                    // Convert BIGINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
                    return (data) -> convertUnsignedBigint(column, fieldDefn, data);
            }
        }

        // We have to convert bytes encoded in the column's character set ...
        switch (column.jdbcType()) {
            case Types.CHAR: // variable-length
            case Types.VARCHAR: // variable-length
            case Types.LONGVARCHAR: // variable-length
            case Types.CLOB: // variable-length
            case Types.NCHAR: // fixed-length
            case Types.NVARCHAR: // fixed-length
            case Types.LONGNVARCHAR: // fixed-length
            case Types.NCLOB: // fixed-length
            case Types.DATALINK:
            case Types.SQLXML:
                Charset charset = charsetFor(column);
                if (charset != null) {
                    logger.debug("Using {} charset by default for column: {}", charset, column);
                    return (data) -> convertString(column, fieldDefn, charset, data);
                }
                logger.warn("Using UTF-8 charset by default for column without charset: {}", column);
                return (data) -> convertString(column, fieldDefn, StandardCharsets.UTF_8, data);
            case Types.TIME:
                if (adaptiveTimeMicrosecondsPrecisionMode) {
                    return data -> convertDurationToMicroseconds(column, fieldDefn, data);
                }
            case Types.TIMESTAMP:
                return ((ValueConverter) (data -> convertTimestampToLocalDateTime(column, fieldDefn, data))).and(super.converter(column, fieldDefn));
            default:
                break;
        }

        // Otherwise, let the base class handle it ...
        return super.converter(column, fieldDefn);
    }

    @Override
    protected ByteOrder byteOrderOfBitType() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    protected Object convertSmallInt(Column column, Field fieldDefn, Object data) {
        // Allows decimal default values for smallint columns
        if (data instanceof String) {
            data = Math.round(Double.parseDouble((String) data));
        }
        return super.convertSmallInt(column, fieldDefn, data);
    }

    @Override
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        // Allows decimal default values for integer columns
        if (data instanceof String) {
            data = Math.round(Double.parseDouble((String) data));
        }
        return super.convertInteger(column, fieldDefn, data);
    }

    @Override
    protected Object convertBigInt(Column column, Field fieldDefn, Object data) {
        // Allows decimal default values for bigint columns
        if (data instanceof String) {
            data = Math.round(Double.parseDouble((String) data));
        }
        return super.convertBigInt(column, fieldDefn, data);
    }

    /**
     * FLOAT(p) values are reported as FLOAT and DOUBLE.<p></p>
     *
     * A value with precision 0 to 23 results in a 4-byte, single-precision FLOAT column.
     * A value with precision 24 to 53 results in an 8-byte, double-precision DOUBLE column.<p></p>
     *
     * MySQL 8.0.17 deprecated the non-standard FLOAT(M,D) and DOUBLE(M,D) syntax, and should expect
     * support for it to be removed in a future release.<p></p>
     *
     * todo: check the above deprecation with regard to MariaDB
     */
    @Override
    protected Object convertFloat(Column column, Field fieldDefn, Object data) {
        if (column.scale().isEmpty() && column.length() <= 24) {
            return super.convertReal(column, fieldDefn, data);
        }
        return super.convertFloat(column, fieldDefn, data);
    }

    @Override
    protected byte[] normalizeBinaryData(Column column, byte[] data) {
        // DBZ-254 right-pad fixed-length binary column values with 0x00 (zero byte)
        if (column.jdbcType() == Types.BINARY && data.length < column.length()) {
            data = Arrays.copyOf(data, column.length());
        }
        return super.normalizeBinaryData(column, data);
    }

    @Override
    protected Object handleUnknownData(Column column, Field fieldDefn, Object data) {
        Class<?> dataClass = data.getClass();
        String clazzName = dataClass.isArray() ? dataClass.getSimpleName() : dataClass.getName();
        // exception will be handled in TableSchemaBuilder.createValueGenerator
        throw new IllegalArgumentException("Unexpected value for JDBC type " + column.jdbcType() + " and column " + column +
                ": class=" + clazzName);
    }

    /**
     * Convert the {@link String} or {@code byte[]} value to a string value used in a {@link SourceRecord}.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param columnCharset the Java character set in which column byte[] values are encoded; may not be null
     * @param data the data; may be null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertString(Column column, Field fieldDefn, Charset columnCharset, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            if (data instanceof byte[]) {
                // Decode the binary representation using the given character encoding ...
                r.deliver(new String((byte[]) data, columnCharset));
            }
            else if (data instanceof String) {
                r.deliver(data);
            }
        });
    }

    /**
     * Convert the {@link String} {@code byte[]} value to a string value used in a {@link SourceRecord}.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertJson(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "{}", (r) -> {
            if (data instanceof byte[]) {
                // The BinlogReader sees these JSON values as binary encoded, so we use the binlog client library's utility
                // to parse the database's internal binary representation into a JSON string, using the standard formatter.
                if (((byte[]) data).length == 0) {
                    r.deliver(column.isOptional() ? null : "{}");
                }
                else {
                    try {
                        r.deliver(JsonBinary.parseAsString((byte[]) data));
                    }
                    catch (IOException e) {
                        if (eventConvertingFailureHandlingMode == FAIL) {
                            Loggings.logErrorAndTraceRecord(logger, Arrays.toString((byte[]) data),
                                    "Failed to parse and read a JSON value on '{}'", column, e);
                            throw new DebeziumException("Failed to parse and read a JSON value on '" + column + "'", e);
                        }
                        Loggings.logWarningAndTraceRecord(logger, Arrays.toString((byte[]) data),
                                "Failed to parse and read a JSON value on '{}'", column, e);
                        r.deliver(column.isOptional() ? null : "{}");
                    }
                }
            }
            else if (data instanceof String) {
                // The SnapshotReader sees JSON values as UTF-8 encoded strings.
                r.deliver(data);
            }
        });
    }

    /**
     * Converts a value object for a {@code YEAR}, which appear in the binlog as an integer though returns from
     * the the JDBC driver as either a short or a {@link java.sql.Date}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a year literal integer value; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    @SuppressWarnings("deprecation")
    protected Object convertYearToInt(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0, (r) -> {
            Object mutData = data;
            if (data instanceof java.time.Year) {
                // The binlog always returns a Year object ...
                r.deliver(adjustTemporal(java.time.Year.of(((java.time.Year) data).getValue())).get(ChronoField.YEAR));
            }
            else if (data instanceof java.sql.Date) {
                // JDBC driver sometimes returns a Java SQL Date object ...
                // year from java.sql.Date is defined as number of years since 1900
                r.deliver(((java.sql.Date) data).getYear() + 1900);
            }
            else if (data instanceof String) {
                mutData = Integer.valueOf((String) data);
            }
            if (mutData instanceof Number) {
                // JDBC driver sometimes returns a short ...
                r.deliver(adjustTemporal(java.time.Year.of(((Number) mutData).intValue())).get(ChronoField.YEAR));
            }
        });
    }

    /**
     * Converts a value object for an {@code ENUM}, which is represented in the binlog events as an integer
     * value containing the index of the enum option. The JDBC driver returns a string containing the option,
     * so this method calculates the same.
     *
     * @param options the characters that appear in the same order as defined in the column; may not be null
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into an {@code ENUM} literal String value
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertEnumToString(List<String> options, Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            if (data instanceof String) {
                // JDBC should return strings ...
                r.deliver(data);
            }
            else if (data instanceof Integer) {
                if (options != null) {
                    // The binlog will contain an int with the 1-based index of the option in the enum value ...
                    int value = ((Integer) data).intValue();
                    if (value == 0) {
                        // an invalid value was specified, which corresponds to the empty string '' and an index of 0
                        r.deliver("");
                    }
                    int index = value - 1; // 'options' is 0-based
                    if (index < options.size() && index >= 0) {
                        r.deliver(options.get(index));
                    }
                }
                else {
                    r.deliver(null);
                }
            }
        });
    }

    /**
     * Converts a value object for a {@code SET}, which is represented in the binlog events contain a long number
     * in which every bit corresponds to a different option. The JDBC driver returns a string containing
     * the comma-separated options, so this method calculates the same.
     *
     * @param options the characters that appear in the same order as defined in the column; may not be null
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into an {@code SET} literal String value; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertSetToString(List<String> options, Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            if (data instanceof String) {
                // JDBC should return strings ...
                r.deliver(data);
            }
            else if (data instanceof Long) {
                // The binlog will contain a long with the indexes of the options in the set value ...
                long indexes = ((Long) data).longValue();
                r.deliver(convertSetValue(column, indexes, options));
            }
        });
    }

    /**
     * Return the {@link Charset} instance with the database-specific character set name used by the given column.
     *
     * @param column the column in which the character set is used; never null
     * @return the Java {@link Charset}, or null if there is no mapping
     */
    protected Charset charsetFor(Column column) {
        String databaseCharSetName = column.charsetName();
        if (databaseCharSetName == null) {
            logger.warn("Column is missing a character set: {}", column);
            return null;
        }
        String encoding = getJavaEncodingForCharSet(databaseCharSetName);
        if (encoding == null) {
            logger.debug("Column uses database character set '{}', which has no mapping to a Java character set, will try it in lowercase", databaseCharSetName);
            encoding = getJavaEncodingForCharSet(databaseCharSetName.toLowerCase());
        }
        if (encoding == null) {
            logger.warn("Column uses database character set '{}', which has no mapping to a Java character set", databaseCharSetName);
        }
        else {
            try {
                return Charset.forName(encoding);
            }
            catch (IllegalCharsetNameException e) {
                logger.error("Unable to load Java charset '{}' for column with database character set '{}'", encoding, databaseCharSetName);
            }
        }
        return null;
    }

    /**
     * Determine if the uppercase form of a column's type exactly matches or begins with the specified prefix.
     * Note that this logic works when the column's {@link Column#typeName() type} contains the type name followed by parentheses.
     *
     * @param upperCaseTypeName the upper case form of the column's {@link Column#typeName() type name}
     * @param upperCaseMatch the upper case form of the expected type or prefix of the type; may not be null
     * @return {@code true} if the type matches the specified type, or {@code false} otherwise
     */
    protected boolean matches(String upperCaseTypeName, String upperCaseMatch) {
        if (upperCaseTypeName == null) {
            return false;
        }
        return upperCaseMatch.equals(upperCaseTypeName) || upperCaseTypeName.startsWith(upperCaseMatch + "(");
    }

    /**
     * Determine if the uppercase form of a column's type is geometry collection independent of JDBC driver or server version.
     *
     * @param upperCaseTypeName the upper case form of the column's {@link Column#typeName() type name}
     * @return {@code true} if the type is geometry collection
     */
    protected boolean isGeometryCollection(String upperCaseTypeName) {
        if (upperCaseTypeName == null) {
            return false;
        }

        return upperCaseTypeName.equals("GEOMETRYCOLLECTION")
                || upperCaseTypeName.equals("GEOMCOLLECTION")
                || upperCaseTypeName.endsWith(".GEOMCOLLECTION");
    }

    protected String extractEnumAndSetOptionsAsString(Column column) {
        return Strings.join(",", extractEnumAndSetOptions(column));
    }

    protected String convertSetValue(Column column, long indexes, List<String> options) {
        StringBuilder sb = new StringBuilder();
        int index = 0;
        boolean first = true;
        int optionLen = options.size();
        while (indexes != 0L) {
            if (indexes % 2L != 0) {
                if (first) {
                    first = false;
                }
                else {
                    sb.append(',');
                }
                if (index < optionLen) {
                    sb.append(options.get(index));
                }
                else {
                    logger.warn("Found unexpected index '{}' on column {}", index, column);
                }
            }
            ++index;
            indexes = indexes >>> 1;
        }
        return sb.toString();
    }

    /**
     * Convert a value representing a POINT {@code byte[]} value to a Point value used in a {@link SourceRecord}.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertPoint(Column column, Field fieldDefn, Object data) {
        final BinlogGeometry empty = BinlogGeometry.createEmpty();
        return convertValue(column, fieldDefn, data, io.debezium.data.geometry.Geometry.createValue(fieldDefn.schema(), empty.getWkb(), empty.getSrid()), (r) -> {
            if (data instanceof byte[]) {
                // The binlog utility sends a byte array for any Geometry type, we will use our own binaryParse to parse the byte to WKB, hence
                // to the suitable class
                BinlogGeometry geometry = BinlogGeometry.fromBytes((byte[]) data);
                if (geometry.isPoint()) {
                    r.deliver(io.debezium.data.geometry.Point.createValue(fieldDefn.schema(), geometry.getWkb(), geometry.getSrid()));
                }
                else {
                    throw new ConnectException("Failed to parse and read a value of type POINT on " + column);
                }
            }
        });
    }

    /**
     * Convert a value representing a GEOMETRY {@code byte[]} value to a Geometry value used in a {@link SourceRecord}.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertGeometry(Column column, Field fieldDefn, Object data) {
        final BinlogGeometry empty = BinlogGeometry.createEmpty();
        return convertValue(column, fieldDefn, data, io.debezium.data.geometry.Geometry.createValue(fieldDefn.schema(), empty.getWkb(), empty.getSrid()), (r) -> {
            if (data instanceof byte[]) {
                // The binlog utility sends a byte array for any Geometry type, we will use our own binaryParse to parse the byte to WKB, hence
                // to the suitable class
                BinlogGeometry geometry = BinlogGeometry.fromBytes((byte[]) data);
                r.deliver(io.debezium.data.geometry.Geometry.createValue(fieldDefn.schema(), geometry.getWkb(), geometry.getSrid()));
            }
        });
    }

    /**
     * Convert a value representing a Unsigned TINYINT value to the correct Unsigned TINYINT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedTinyint(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, (short) 0, (r) -> {
            if (data instanceof Short) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedTinyint((short) data));
            }
            else if (data instanceof Number) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedTinyint(((Number) data).shortValue()));
            }
            else {
                // We continue with the original converting method (smallint) since we have an unsigned Tinyint
                r.deliver(convertSmallInt(column, fieldDefn, data));
            }
        });
    }

    /**
     * Convert a value representing a Unsigned SMALLINT value to the correct Unsigned SMALLINT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedSmallint(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0, (r) -> {
            if (data instanceof Integer) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedSmallint((int) data));
            }
            else if (data instanceof Number) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedSmallint(((Number) data).intValue()));
            }
            else {
                // We continue with the original converting method (integer) since we have an unsigned Smallint
                r.deliver(convertInteger(column, fieldDefn, data));
            }
        });
    }

    /**
     * Convert a value representing a Unsigned MEDIUMINT value to the correct Unsigned SMALLINT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedMediumint(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0, (r) -> {
            if (data instanceof Integer) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedMediumint((int) data));
            }
            else if (data instanceof Number) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedMediumint(((Number) data).intValue()));
            }
            else {
                // We continue with the original converting method (integer) since we have an unsigned Medium
                r.deliver(convertInteger(column, fieldDefn, data));
            }
        });
    }

    /**
     * Convert a value representing a Unsigned INT value to the correct Unsigned INT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedInt(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            if (data instanceof Long) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedInteger((long) data));
            }
            else if (data instanceof Number) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedInteger(((Number) data).longValue()));
            }
            else {
                // We continue with the original converting method (bigint) since we have an unsigned Integer
                r.deliver(convertBigInt(column, fieldDefn, data));
            }
        });
    }

    /**
     * Convert a value representing a Unsigned BIGINT value to the correct Unsigned INT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedBigint(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            if (data instanceof BigDecimal) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedBigint((BigDecimal) data));
            }
            else if (data instanceof Number) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedBigint(new BigDecimal(((Number) data).toString())));
            }
            else if (data instanceof String) {
                r.deliver(BinlogUnsignedIntegerConverter.convertUnsignedBigint(new BigDecimal((String) data)));
            }
            else {
                r.deliver(convertNumeric(column, fieldDefn, data));
            }
        });
    }

    /**
     * Converts a value object for an expected type of {@link java.time.Duration} to {@link Long} values that represents
     * the time in microseconds.<p></p>
     *
     * Per the JDBC specification, databases should return {@link java.sql.Time} instances, but that's not working
     * because it can only handle Daytime 00:00:00-23:59:59. We use {@link java.time.Duration} instead that can handle
     * the range of -838:59:59.000000 to 838:59:59.000000 of a TIME type and transfer data as signed INT64 which
     * reflects the DB value converted to microseconds.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link java.time.Duration} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertDurationToMicroseconds(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            try {
                if (data instanceof Duration) {
                    r.deliver(((Duration) data).toNanos() / 1_000);
                }
            }
            catch (IllegalArgumentException e) {
            }
        });
    }

    protected Object convertTimestampToLocalDateTime(Column column, Field fieldDefn, Object data) {
        if (data == null && !fieldDefn.schema().isOptional()) {
            return null;
        }
        if (!(data instanceof Timestamp)) {
            return data;
        }
        return ((Timestamp) data).toLocalDateTime();
    }

    protected abstract List<String> extractEnumAndSetOptions(Column column);

    protected String getJavaEncodingForCharSet(String charSetName) {
        return charsetRegistry.getJavaEncodingForCharSet(charSetName);
    }

    /**
     * A utility method that adjusts <a href="https://dev.mysql.com/doc/refman/8.2/en/two-digit-years.html">ambiguous</a> 2-digit
     * year values of DATETIME, DATE, and TIMESTAMP types using these database-specific rules:
     * <ul>
     * <li>Year values in the range 00-69 are converted to 2000-2069.</li>
     * <li>Year values in the range 70-99 are converted to 1970-1999.</li>
     * </ul>
     *
     * @param temporal the temporal instance to adjust; may not be null
     * @return the possibly adjusted temporal instance; never null
     */
    public static Temporal adjustTemporal(Temporal temporal) {
        if (temporal.isSupported(ChronoField.YEAR)) {
            int year = temporal.get(ChronoField.YEAR);
            if (0 <= year && year <= 69) {
                temporal = temporal.plus(2000, ChronoUnit.YEARS);
            }
            else if (70 <= year && year <= 99) {
                temporal = temporal.plus(1900, ChronoUnit.YEARS);
            }
        }
        return temporal;
    }

    /**
     * Converts a {@link #TIME_FIELD_PATTERN} time field pattern string to a {@link Duration}.
     *
     * @param timeString the time field string value; should not be null
     * @return the parsed duration
     */
    public static Duration stringToDuration(String timeString) {
        final Matcher matcher = TIME_FIELD_PATTERN.matcher(timeString);
        if (!matcher.matches()) {
            throw new DebeziumException("Unexpected format for TIME column: " + timeString);
        }

        final boolean isNegative = !timeString.isBlank() && timeString.charAt(0) == '-';
        final long hours = Long.parseLong(matcher.group(1));
        final long minutes = Long.parseLong(matcher.group(2));
        final String secondsGroup = matcher.group(4);

        long seconds = 0;
        long nanoSeconds = 0;

        if (!Objects.isNull(secondsGroup)) {
            seconds = Long.parseLong(secondsGroup);
            String microSecondsString = matcher.group(6);
            if (!Objects.isNull(microSecondsString)) {
                nanoSeconds = Long.parseLong(Strings.justifyLeft(microSecondsString, 9, '0'));
            }
        }

        final Duration duration = hours >= 0
                ? Duration
                        .ofHours(hours)
                        .plusMinutes(minutes)
                        .plusSeconds(seconds)
                        .plusNanos(nanoSeconds)
                : Duration
                        .ofHours(hours)
                        .minusMinutes(minutes)
                        .minusSeconds(seconds)
                        .minusNanos(nanoSeconds);
        return isNegative && !duration.isNegative() ? duration.negated() : duration;
    }

    /**
     * Converts a {@link #DATE_FIELD_PATTERN} date string field pattern to a {@link LocalDate} value.
     *
     * @param dateString date string to be parsed; may be null or empty
     * @param column the relational column, should not be null
     * @param table the relational table, should not be null
     * @return the parsed local date or null if the date's year, month, or day are zero
     */
    public static LocalDate stringToLocalDate(String dateString, Column column, Table table) {
        final Matcher matcher = DATE_FIELD_PATTERN.matcher(dateString);
        if (!matcher.matches()) {
            throw new RuntimeException("Unexpected format for DATE column: " + dateString);
        }

        final int year = Integer.parseInt(matcher.group(1));
        final int month = Integer.parseInt(matcher.group(2));
        final int day = Integer.parseInt(matcher.group(3));

        if (month == 0 || day == 0) {
            INVALID_VALUE_LOGGER.warn("Invalid value '{}' stored in column '{}' of table '{}' converted to empty value",
                    dateString, column.name(), table.id());
            return null;
        }
        return LocalDate.of(year, month, day);
    }

    /**
     * Checks if the {@link #TIMESTAMP_FIELD_PATTERN} timestamp field pattern contains {@code 0} in its
     * year, month, or day parts of the parsed value.
     *
     * @param timestampString timestamp string to be parsed, may be null or empty
     * @param column the relational column, should not be null
     * @param table the relational table, should not be null
     * @return true if the timestamp's year, month, or day are zero; false otherwise
     */
    public static boolean containsZeroValuesInDatePart(String timestampString, Column column, Table table) {
        final Matcher matcher = TIMESTAMP_FIELD_PATTERN.matcher(timestampString);
        if (!matcher.matches()) {
            throw new RuntimeException("Unexpected format for DATE column: " + timestampString);
        }

        final int year = Integer.parseInt(matcher.group(1));
        final int month = Integer.parseInt(matcher.group(2));
        final int day = Integer.parseInt(matcher.group(3));

        if (month == 0 || day == 0) {
            INVALID_VALUE_LOGGER.warn("Invalid value '{}' stored in column '{}' of table '{}' converted to empty value",
                    timestampString, column.name(), table.id());
            return true;
        }
        return false;
    }
}
