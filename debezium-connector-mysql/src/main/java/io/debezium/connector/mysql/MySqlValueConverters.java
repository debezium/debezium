/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import com.mysql.jdbc.CharsetMapping;

import io.debezium.annotation.Immutable;
import io.debezium.data.Json;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Year;
import io.debezium.util.Strings;
import mil.nga.wkb.geom.Point;
import mil.nga.wkb.util.WkbException;

/**
 * MySQL-specific customization of the conversions from JDBC values obtained from the MySQL binlog client library.
 * <p>
 * This class always uses UTC for the default time zone when converting values without timezone information to values that require
 * timezones. This is because MySQL {@code TIMESTAMP} values are always
 * <a href="https://dev.mysql.com/doc/refman/5.7/en/datetime.html">stored in UTC</a> (unlike {@code DATETIME} values) and
 * are replicated in this form. Meanwhile, the MySQL Binlog Client library will {@link AbstractRowsEventDataDeserializer
 * deserialize} these as {@link java.sql.Timestamp} values that have no timezone and, therefore, are presumed to be in UTC.
 * When the column is properly marked with a {@link Types#TIMESTAMP_WITH_TIMEZONE} type, the converters will need to convert
 * that {@link java.sql.Timestamp} value into an {@link OffsetDateTime} using the default time zone, which always is UTC.
 *
 * @author Randall Hauch
 * @see com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer
 */
@Immutable
public class MySqlValueConverters extends JdbcValueConverters {

    /**
     * A utility method that adjusts <a href="https://dev.mysql.com/doc/refman/5.7/en/two-digit-years.html">ambiguous</a> 2-digit
     * year values of DATETIME, DATE, and TIMESTAMP types using these MySQL-specific rules:
     * <ul>
     * <li>Year values in the range 00-69 are converted to 2000-2069.</li>
     * <li>Year values in the range 70-99 are converted to 1970-1999.</li>
     * </ul>
     *
     * @param temporal the temporal instance to adjust; may not be null
     * @return the possibly adjusted temporal instance; never null
     */
    protected static Temporal adjustTemporal(Temporal temporal) {
        if (temporal.isSupported(ChronoField.YEAR)) {
            int year = temporal.get(ChronoField.YEAR);
            if (0 <= year && year <= 69) {
                temporal = temporal.plus(2000, ChronoUnit.YEARS);
            } else if (70 <= year && year <= 99) {
                temporal = temporal.plus(1900, ChronoUnit.YEARS);
            }
        }
        return temporal;
    }

    /**
     * A utility method that adjusts <a href="https://dev.mysql.com/doc/refman/5.7/en/two-digit-years.html">ambiguous</a> 2-digit
     * year values of YEAR type using these MySQL-specific rules:
     * <ul>
     * <li>Year values in the range 01-69 are converted to 2001-2069.</li>
     * <li>Year values in the range 70-99 are converted to 1970-1999.</li>
     * </ul>
     * MySQL treats YEAR(4) the same, except that a numeric 00 inserted into YEAR(4) results in 0000 rather than 2000; to
     * specify zero for YEAR(4) and have it be interpreted as 2000, specify it as a string '0' or '00'. This should be handled
     * by MySQL before Debezium sees the value.
     *
     * @param year the year value to adjust; may not be null
     * @return the possibly adjusted year number; never null
     */
    protected static int adjustYear(int year) {
        if (0 < year && year <= 69) {
            year += 2000;
        } else if (70 <= year && year <= 99) {
            year += 1900;
        }
        return year;
    }

    /**
     * Create a new instance that always uses UTC for the default time zone when converting values without timezone information
     * to values that require timezones.
     * <p>
     *
     * @param decimalMode how {@code DECIMAL} and {@code NUMERIC} values should be treated; may be null if
     *            {@link io.debezium.jdbc.JdbcValueConverters.DecimalMode#PRECISE} is to be used
     * @param temporalPrecisionMode temporal precision mode based on {@link io.debezium.jdbc.TemporalPrecisionMode}
     * @param bigIntUnsignedMode how {@code BIGINT UNSIGNED} values should be treated; may be null if
     *            {@link io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode#PRECISE} is to be used
     */
    public MySqlValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, BigIntUnsignedMode bigIntUnsignedMode) {
        this(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, bigIntUnsignedMode);
    }

    /**
     * Create a new instance, and specify the time zone offset that should be used only when converting values without timezone
     * information to values that require timezones. This default offset should not be needed when values are highly-correlated
     * with the expected SQL/JDBC types.
     *
     * @param decimalMode how {@code DECIMAL} and {@code NUMERIC} values should be treated; may be null if
     *            {@link io.debezium.jdbc.JdbcValueConverters.DecimalMode#PRECISE} is to be used
     * @param temporalPrecisionMode temporal precision mode based on {@link io.debezium.jdbc.TemporalPrecisionMode}
     * @param defaultOffset the zone offset that is to be used when converting non-timezone related values to values that do
     *            have timezones; may be null if UTC is to be used
     * @param bigIntUnsignedMode how {@code BIGINT UNSIGNED} values should be treated; may be null if
     *            {@link io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode#PRECISE} is to be used
     */
    public MySqlValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset, BigIntUnsignedMode bigIntUnsignedMode) {
        super(decimalMode, temporalPrecisionMode, defaultOffset, MySqlValueConverters::adjustTemporal, bigIntUnsignedMode);
    }

    @Override
    protected ByteOrder byteOrderOfBitType() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        // Handle a few MySQL-specific types based upon how they are handled by the MySQL binlog client ...
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "JSON")) {
            return Json.builder();
        }
        if (matches(typeName, "POINT")) {
            return io.debezium.data.geometry.Point.builder();
        }
        if (matches(typeName, "YEAR")) {
            return Year.builder();
        }
        if (matches(typeName, "ENUM")) {
            String commaSeperatedOptions = extractEnumAndSetOptionsAsString(column);
            return io.debezium.data.Enum.builder(commaSeperatedOptions);
        }
        if (matches(typeName, "SET")) {
            String commaSeperatedOptions = extractEnumAndSetOptionsAsString(column);
            return io.debezium.data.EnumSet.builder(commaSeperatedOptions);
        }
        if (matches(typeName, "SMALLINT UNSIGNED") || matches(typeName, "SMALLINT UNSIGNED ZEROFILL")) {
            // In order to capture unsigned SMALLINT 16-bit data source, INT32 will be required to safely capture all valid values
            // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
            return SchemaBuilder.int32();
        }
        if (matches(typeName, "INT UNSIGNED") || matches(typeName, "INT UNSIGNED ZEROFILL")) {
            // In order to capture unsigned INT 32-bit data source, INT64 will be required to safely capture all valid values
            // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
            return SchemaBuilder.int64();
        }
        if (matches(typeName, "BIGINT UNSIGNED") || matches(typeName, "BIGINT UNSIGNED ZEROFILL")) {
            switch (super.bigIntUnsignedMode) {
                case LONG:
                    return SchemaBuilder.int64();
                case PRECISE:
                    // In order to capture unsigned INT 64-bit data source, org.apache.kafka.connect.data.Decimal:Byte will be required to safely capture all valid values with scale of 0
                    // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
                    return Decimal.builder(0);
            }
        }
        // Otherwise, let the base class handle it ...
        return super.schemaBuilder(column);
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        // Handle a few MySQL-specific types based upon how they are handled by the MySQL binlog client ...
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "JSON")) {
            return (data) -> convertJson(column, fieldDefn, data);
        }
        if (matches(typeName, "POINT")){
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
        if (matches(typeName, "TINYINT UNSIGNED") || matches(typeName, "TINYINT UNSIGNED ZEROFILL")) {
            // Convert TINYINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
            return (data) -> convertUnsignedTinyint(column, fieldDefn, data);
        }
        if (matches(typeName, "SMALLINT UNSIGNED") || matches(typeName, "SMALLINT UNSIGNED ZEROFILL")) {
            // Convert SMALLINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
            return (data) -> convertUnsignedSmallint(column, fieldDefn, data);
        }
        if (matches(typeName, "MEDIUMINT UNSIGNED") || matches(typeName, "MEDIUMINT UNSIGNED ZEROFILL")) {
            // Convert SMALLINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
            return (data) -> convertUnsignedMediumint(column, fieldDefn, data);
        }
        if (matches(typeName, "INT UNSIGNED") || matches(typeName, "INT UNSIGNED ZEROFILL")) {
            // Convert INT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
            return (data) -> convertUnsignedInt(column, fieldDefn, data);
        }
        if (matches(typeName, "BIGINT UNSIGNED") || matches(typeName, "BIGINT UNSIGNED ZEROFILL")) {
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
                if (adaptiveTimeMicrosecondsPrecisionMode)
                    return data -> convertDurationToMicroseconds(column, fieldDefn, data);
            default:
                break;
        }

        // Otherwise, let the base class handle it ...
        return super.converter(column, fieldDefn);
    }

    /**
     * Return the {@link Charset} instance with the MySQL-specific character set name used by the given column.
     *
     * @param column the column in which the character set is used; never null
     * @return the Java {@link Charset}, or null if there is no mapping
     */
    protected Charset charsetFor(Column column) {
        String mySqlCharsetName = column.charsetName();
        if (mySqlCharsetName == null) {
            logger.warn("Column is missing a character set: {}", column);
            return null;
        }
        String encoding = CharsetMapping.getJavaEncodingForMysqlCharset(mySqlCharsetName);
        if (encoding == null) {
            logger.warn("Column uses MySQL character set '{}', which has no mapping to a Java character set", mySqlCharsetName);
        } else {
            try {
                return Charset.forName(encoding);
            } catch (IllegalCharsetNameException e) {
                logger.error("Unable to load Java charset '{}' for column with MySQL character set '{}'", encoding, mySqlCharsetName);
            }
        }
        return null;
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
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return "{}";
        }
        if (data instanceof byte[]) {
            // The BinlogReader sees these JSON values as binary encoded, so we use the binlog client library's utility
            // to parse MySQL's internal binary representation into a JSON string, using the standard formatter.
            try {
                String json = JsonBinary.parseAsString((byte[]) data);
                return json;
            } catch (IOException e) {
                throw new ConnectException("Failed to parse and read a JSON value on " + column + ": " + e.getMessage(), e);
            }
        }
        if (data instanceof String) {
            // The SnapshotReader sees JSON values as UTF-8 encoded strings.
            return data;
        }
        return handleUnknownData(column, fieldDefn, data);
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
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return "";
        }
        if (data instanceof byte[]) {
            // Decode the binary representation using the given character encoding ...
            return new String((byte[]) data, columnCharset);
        }
        if (data instanceof String) {
            return data;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for a MySQL {@code YEAR}, which appear in the binlog as an integer though returns from
     * the MySQL JDBC driver as either a short or a {@link java.sql.Date}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a year literal integer value; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    @SuppressWarnings("deprecation")
    protected Object convertYearToInt(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0;
        }
        if (data instanceof java.time.Year) {
            // The MySQL binlog always returns a Year object ...
            return adjustYear(((java.time.Year) data).getValue());
        }
        if (data instanceof java.sql.Date) {
            // MySQL JDBC driver sometimes returns a Java SQL Date object ...
            return adjustYear(((java.sql.Date) data).getYear());
        }
        if (data instanceof Number) {
            // MySQL JDBC driver sometimes returns a short ...
            return adjustYear(((Number) data).intValue());
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for a MySQL {@code ENUM}, which is represented in the binlog events as an integer value containing
     * the index of the enum option. The MySQL JDBC driver returns a string containing the option,
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
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return "";
        }
        if (data instanceof String) {
            // JDBC should return strings ...
            return data;
        }
        if (data instanceof Integer) {

            if (options != null) {
                // The binlog will contain an int with the 1-based index of the option in the enum value ...
                int value = ((Integer) data).intValue();
                if (value == 0) {
                    // an invalid value was specified, which corresponds to the empty string '' and an index of 0
                    return "";
                }
                int index = value - 1; // 'options' is 0-based
                if (index < options.size() && index >= 0) {
                    return options.get(index);
                }
            }
            return null;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for a MySQL {@code SET}, which is represented in the binlog events contain a long number in which
     * every bit corresponds to a different option. The MySQL JDBC driver returns a string containing the comma-separated options,
     * so this method calculates the same.
     *
     * @param options the characters that appear in the same order as defined in the column; may not be null
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into an {@code SET} literal String value; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertSetToString(List<String> options, Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return "";
        }
        if (data instanceof String) {
            // JDBC should return strings ...
            return data;
        }
        if (data instanceof Long) {
            // The binlog will contain a long with the indexes of the options in the set value ...
            long indexes = ((Long) data).longValue();
            return convertSetValue(column, indexes, options);
        }
        return handleUnknownData(column, fieldDefn, data);
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
        if (upperCaseTypeName == null) return false;
        return upperCaseMatch.equals(upperCaseTypeName) || upperCaseTypeName.startsWith(upperCaseMatch + "(");
    }

    protected List<String> extractEnumAndSetOptions(Column column) {
        return MySqlDdlParser.parseSetAndEnumOptions(column.typeExpression());
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
                } else {
                    sb.append(',');
                }
                if (index < optionLen) {
                    sb.append(options.get(index));
                } else {
                    logger.warn("Found unexpected index '{}' on column {}", index, column);
                }
            }
            ++index;
            indexes = indexes >>> 1;
        }
        return sb.toString();

    }

    /**
     * Convert the a value representing a POINT {@code byte[]} value to a Point value used in a {@link SourceRecord}.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertPoint(Column column, Field fieldDefn, Object data){
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }

        Schema schema = fieldDefn.schema();

        if (data == null) {
            if (column.isOptional()) return null;
            return io.debezium.data.geometry.Point.createValue(schema, 0.0, 0.0);
        }

        if (data instanceof byte[]) {
            // The binlog utility sends a byte array for any Geometry type, we will use our own binaryParse to parse the byte to WKB, hence
            // to the suitable class
            try {
                MySqlGeometry mySqlGeometry = MySqlGeometry.fromBytes((byte[]) data);
                Point point = mySqlGeometry.getPoint();
                return io.debezium.data.geometry.Point.createValue(schema, point.getX(), point.getY(), mySqlGeometry.getWkb());
            } catch (WkbException e) {
                throw new ConnectException("Failed to parse and read a value of type POINT on " + column + ": " + e.getMessage(), e);
            }
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    @Override
    protected ByteBuffer convertByteArray(Column column, byte[] data) {
        // DBZ-254 right-pad fixed-length binary column values with 0x00 (zero byte)
        if (column.jdbcType() == Types.BINARY && data.length < column.length()) {
            data = Arrays.copyOf(data, column.length());
        }

        return super.convertByteArray(column, data);
    }

    /**
     * Convert the a value representing a Unsigned TINYINT value to the correct Unsigned TINYINT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedTinyint(Column column, Field fieldDefn, Object data){
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0;
        }

        if (data instanceof Short) {
            return MySqlUnsignedIntegerConverter.convertUnsignedTinyint((short) data);
        }
        else if (data instanceof Number) {
            return MySqlUnsignedIntegerConverter.convertUnsignedTinyint(((Number) data).shortValue());
        }
        else {
            //We continue with the original converting method (smallint) since we have an unsigned Tinyint
            return convertSmallInt(column, fieldDefn, data);
        }
    }

    /**
     * Convert the a value representing a Unsigned SMALLINT value to the correct Unsigned SMALLINT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedSmallint(Column column, Field fieldDefn, Object data){
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0;
        }

        if (data instanceof Integer) {
            return MySqlUnsignedIntegerConverter.convertUnsignedSmallint((int)data);
        }
        else if (data instanceof Number) {
            return MySqlUnsignedIntegerConverter.convertUnsignedSmallint(((Number) data).intValue());
        }
        else {
            //We continue with the original converting method (integer) since we have an unsigned Smallint
            return convertInteger(column, fieldDefn, data);
        }
    }

    /**
     * Convert the a value representing a Unsigned MEDIUMINT value to the correct Unsigned SMALLINT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedMediumint(Column column, Field fieldDefn, Object data){
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0;
        }

        if (data instanceof Integer) {
            return MySqlUnsignedIntegerConverter.convertUnsignedMediumint((int)data);
        }
        else if (data instanceof Number) {
            return MySqlUnsignedIntegerConverter.convertUnsignedMediumint(((Number) data).intValue());
        }
        else {
            //We continue with the original converting method (integer) since we have an unsigned Medium
            return convertInteger(column, fieldDefn, data);
        }
    }

    /**
     * Convert the a value representing a Unsigned INT value to the correct Unsigned INT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedInt(Column column, Field fieldDefn, Object data){
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0;
        }

        if (data instanceof Long) {
            return MySqlUnsignedIntegerConverter.convertUnsignedInteger((long) data);
        }
        else if (data instanceof Number) {
            return MySqlUnsignedIntegerConverter.convertUnsignedInteger(((Number) data).longValue());
        }
        else {
            //We continue with the original converting method (bigint) since we have an unsigned Integer
            return convertBigInt(column, fieldDefn, data);
        }
    }

    /**
     * Convert the a value representing a Unsigned BIGINT value to the correct Unsigned INT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedBigint(Column column, Field fieldDefn, Object data){
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0;
        }

        if (data instanceof BigDecimal) {
            return MySqlUnsignedIntegerConverter.convertUnsignedBigint((BigDecimal) data);
        }
        else if (data instanceof Number) {
            return MySqlUnsignedIntegerConverter.convertUnsignedBigint(new BigDecimal(((Number) data).toString()));
        }
        else {
            //We continue with the original converting method (numeric) since we have an unsigned Integer
            return convertNumeric(column, fieldDefn, data);
        }
    }

    /**
     * Converts a value object for an expected type of {@link java.time.Duration} to {@link Long} values that represents
     * the time in microseconds.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Time} instances, but that's not working
     * because it can only handle Daytime 00:00:00-23:59:59. We use {@link java.time.Duration} instead that can handle
     * the range of -838:59:59.000000 to 838:59:59.000000 of a MySQL TIME type and transfer data as signed INT64 which
     * reflects the DB value converted to microseconds.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link java.time.Duration} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertDurationToMicroseconds(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0;
        }
        try {
            if (data instanceof Duration) return ((Duration) data).toNanos() / 1_000;
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
        return handleUnknownData(column, fieldDefn, data);
    }
}
