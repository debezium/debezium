/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;

import com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer;
import com.mysql.jdbc.CharsetMapping;

import io.debezium.annotation.Immutable;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Year;

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
     * Create a new instance that always uses UTC for the default time zone when converting values without timezone information
     * to values that require timezones.
     * <p>
     * 
     * @param adaptiveTimePrecision {@code true} if the time, date, and timestamp values should be based upon the precision of the
     *            database columns using {@link io.debezium.time} semantic types, or {@code false} if they should be fixed to
     *            millisecond precision using Kafka Connect {@link org.apache.kafka.connect.data} logical types.
     */
    public MySqlValueConverters(boolean adaptiveTimePrecision) {
        this(adaptiveTimePrecision, ZoneOffset.UTC);
    }

    /**
     * Create a new instance, and specify the time zone offset that should be used only when converting values without timezone
     * information to values that require timezones. This default offset should not be needed when values are highly-correlated
     * with the expected SQL/JDBC types.
     * 
     * @param adaptiveTimePrecision {@code true} if the time, date, and timestamp values should be based upon the precision of the
     *            database columns using {@link io.debezium.time} semantic types, or {@code false} if they should be fixed to
     *            millisecond precision using Kafka Connect {@link org.apache.kafka.connect.data} logical types.
     * @param defaultOffset the zone offset that is to be used when converting non-timezone related values to values that do
     *            have timezones; may be null if UTC is to be used
     */
    public MySqlValueConverters(boolean adaptiveTimePrecision, ZoneOffset defaultOffset) {
        super(adaptiveTimePrecision, defaultOffset);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        // Handle a few MySQL-specific types based upon how they are handled by the MySQL binlog client ...
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "YEAR")) {
            return Year.builder();
        }
        if (matches(typeName, "ENUM")) {
            String commaSeparatedOptions = extractEnumAndSetOptions(column, true);
            return io.debezium.data.Enum.builder(commaSeparatedOptions);
        }
        if (matches(typeName, "SET")) {
            String commaSeparatedOptions = extractEnumAndSetOptions(column, true);
            return io.debezium.data.EnumSet.builder(commaSeparatedOptions);
        }
        // Otherwise, let the base class handle it ...
        return super.schemaBuilder(column);
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        // Handle a few MySQL-specific types based upon how they are handled by the MySQL binlog client ...
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "YEAR")) {
            return (data) -> convertYearToInt(column, fieldDefn, data);
        }
        if (matches(typeName, "ENUM")) {
            // Build up the character array based upon the column's type ...
            String options = extractEnumAndSetOptions(column, false);
            return (data) -> convertEnumToString(options, column, fieldDefn, data);
        }
        if (matches(typeName, "SET")) {
            // Build up the character array based upon the column's type ...
            String options = extractEnumAndSetOptions(column, false);
            return (data) -> convertSetToString(options, column, fieldDefn, data);
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
     * Convert the {@link String} or {@code byte[]} value to a string value used in a {@link SourceRecord}.
     * 
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never null
     * @param columnCharset the Java character set in which column byte[] values are encoded; may not be null
     * @param data the data; may be null
     * @return the string value; may be null if the value is null or is an unknown input type
     */
    protected Object convertString(Column column, Field fieldDefn, Charset columnCharset, Object data) {
        if (data == null) return null;
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
     * @return the converted value, or null if the conversion could not be made
     */
    @SuppressWarnings("deprecation")
    protected Object convertYearToInt(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof java.time.Year) {
            // The MySQL binlog always returns a Year object ...
            return ((java.time.Year) data).getValue();
        }
        if (data instanceof java.sql.Date) {
            // MySQL JDBC driver sometimes returns a Java SQL Date object ...
            return ((java.sql.Date) data).getYear();
        }
        if (data instanceof Number) {
            // MySQL JDBC driver sometimes returns a short ...
            return ((Number) data).intValue();
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
     * @param data the data object to be converted into an {@code ENUM} literal String value; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertEnumToString(String options, Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof String) {
            // JDBC should return strings ...
            return data;
        }
        if (data instanceof Integer) {
            // The binlog will contain an int with the 1-based index of the option in the enum value ...
            int index = ((Integer) data).intValue() - 1; // 'options' is 0-based
            if (index < options.length()) {
                return options.substring(index, index + 1);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertSetToString(String options, Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof String) {
            // JDBC should return strings ...
            return data;
        }
        if (data instanceof Long) {
            // The binlog will contain a long with the indexes of the options in the set value ...
            long indexes = ((Long) data).longValue();
            return convertSetValue(indexes, options);
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

    protected String extractEnumAndSetOptions(Column column, boolean commaSeparated) {
        String options = MySqlDdlParser.parseSetAndEnumOptions(column.typeExpression());
        if (!commaSeparated) return options;
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (int i = 0; i != options.length(); ++i) {
            if (first)
                first = false;
            else
                sb.append(',');
            sb.append(options.charAt(i));
        }
        return sb.toString();
    }

    protected String convertSetValue(long indexes, String options) {
        StringBuilder sb = new StringBuilder();
        int index = 0;
        boolean first = true;
        while (indexes != 0L) {
            if (indexes % 2L != 0) {
                if (first)
                    first = false;
                else
                    sb.append(',');
                sb.append(options.substring(index, index + 1));
            }
            ++index;
            indexes = indexes >>> 1;
        }
        return sb.toString();

    }

}
