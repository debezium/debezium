/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.protocol.a.NativeConstants;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Decode binary protocol value for MySQL.
 *
 * Consult <a href="https://dev.mysql.com/doc/internals/en/binary-protocol-value.html">Binary Protocol Value</a> if you want to learn more about this.
 *
 * @author yangjie
 */
public class MySqlBinaryProtocolFieldReader extends AbstractFieldReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlBinaryProtocolFieldReader.class);

    public MySqlBinaryProtocolFieldReader(MySqlConnectorConfig config) {
        super(config);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_TIME">ProtocolBinary::MYSQL_TYPE_TIME</a>
     */
    @Override
    protected Object readTimeField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        Blob b = rs.getBlob(columnIndex);
        if (b == null) {
            return null; // Don't continue parsing time field if it is null
        }
        else if (b.length() == 0) {
            // The MySQL binary protocol encodes TIME 00:00:00 as a zero-length value (all components zero).
            return Duration.ZERO;
        }

        // TODO Connector/J 9.7.0 fixed the decoding of negative TIME values (Bug #119863). Once the driver is upgraded to 9.7.0
        // or later, rs.getObject(columnIndex, Duration.class) returns the correct value for both the binary and the text protocol,
        // so it can replace the decoding below.
        final byte[] bytes = b.getBytes(1, (int) (b.length()));
        if (!isBinaryTime(bytes)) {
            // A statement without a server-side cursor, such as the maximum key query of an incremental snapshot,
            // returns the text representation of the value
            final String value = new String(bytes, StandardCharsets.UTF_8);
            try {
                return MySqlValueConverters.stringToDuration(value);
            }
            catch (RuntimeException e) {
                logInvalidValue(rs, columnIndex, value);
                LOGGER.error("Could not read MySQL TIME value of length {}. " +
                        "Enable TRACE logging to log the problematic column and its value.", bytes.length);
                throw e;
            }
        }

        // The value consists of the sign, days, hours, minutes, seconds and, if its length is 12, microseconds
        final Duration duration = Duration.ofDays(readUnsignedInt(bytes, 1))
                .plusHours(bytes[5])
                .plusMinutes(bytes[6])
                .plusSeconds(bytes[7])
                .plusNanos(bytes.length == NativeConstants.BIN_LEN_TIME_WITH_MICROS ? readUnsignedInt(bytes, 8) * 1_000 : 0);
        return bytes[0] == 1 ? duration.negated() : duration;
    }

    private static boolean isBinaryTime(byte[] bytes) {
        // A binary value starts with the sign flag 0 or 1, while a text value starts with a digit or '-'
        return (bytes.length == NativeConstants.BIN_LEN_TIME_NO_FRAC || bytes.length == NativeConstants.BIN_LEN_TIME_WITH_MICROS)
                && (bytes[0] == 0 || bytes[0] == 1);
    }

    private static long readUnsignedInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFFL) | ((bytes[offset + 1] & 0xFFL) << 8) | ((bytes[offset + 2] & 0xFFL) << 16) | ((bytes[offset + 3] & 0xFFL) << 24);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_DATE">ProtocolBinary::MYSQL_TYPE_DATE</a>
     */
    @Override
    protected Object readDateField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        Blob b = rs.getBlob(columnIndex);
        if (b == null) {
            return null; // Don't continue parsing date field if it is null
        }
        else if (b.length() == 0) {
            // Zero date has zero length when binary protocol uses compression.
            if (isStructuredTemporalMode()) {
                return MySqlValueConverters.stringToBinlogDateValue("0000-00-00");
            }
            return column.isOptional() ? null : LocalDate.EPOCH;
        }
        // length is 4
        if (b.length() != NativeConstants.BIN_LEN_DATE) {
            logInvalidValue(rs, columnIndex, b);
            throw new RuntimeException(String.format("Invalid length when read MySQL DATE value. BIN_LEN_DATE is %d. " +
                    "Enable TRACE logging to log the problematic column and its value.", b.length()));
        }

        final byte[] bytes = b.getBytes(1L, (int) b.length());
        final int year = (bytes[0] & 0xFF) | ((bytes[1] & 0xFF) << 8);
        final int month = bytes[2];
        final int day = bytes[3];

        final String value = String.format("%d-%d-%d", year, month, day);
        if (isStructuredTemporalMode()) {
            return MySqlValueConverters.stringToBinlogDateValue(value);
        }
        return MySqlValueConverters.stringToLocalDate(value, column, table);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_DATETIME">ProtocolBinary::MYSQL_TYPE_DATETIME</a>
     */
    @Override
    protected Object readTimestampField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        Blob b = rs.getBlob(columnIndex);
        if (b == null) {
            return null; // Don't continue parsing timestamp field if it is null
        }
        else if (b.length() == 0) {
            LOGGER.warn("Encountered a zero length blob for column index {}", columnIndex);
            return isStructuredTemporalMode() ? MySqlValueConverters.stringToBinlogDateTimeValue("0000-00-00 00:00:00") : null;
        }

        // if hour, minutes, seconds and micro_seconds are all 0, length is 4; if micro_seconds is 0, length is 7; otherwise length is 11
        if (b.length() != NativeConstants.BIN_LEN_DATE && b.length() != NativeConstants.BIN_LEN_TIMESTAMP_NO_FRAC
                && b.length() != NativeConstants.BIN_LEN_TIMESTAMP_WITH_MICROS) {
            logInvalidValue(rs, columnIndex, b);
            throw new RuntimeException(String.format("Invalid length when read MySQL DATETIME value. BIN_LEN_DATETIME is %d. " +
                    "Enable TRACE logging to log the problematic column and its value.", b.length()));
        }

        final byte[] bytes = b.getBytes(1, (int) (b.length()));
        final int year = (bytes[0] & 0xFF) | ((bytes[1] & 0xFF) << 8);
        final int month = bytes[2];
        final int day = bytes[3];
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int nanos = 0;
        if (bytes.length > NativeConstants.BIN_LEN_DATE) {
            hours = bytes[4];
            minutes = bytes[5];
            seconds = bytes[6];
        }
        if (bytes.length > NativeConstants.BIN_LEN_TIMESTAMP_NO_FRAC) {
            nanos = 1000 * ((bytes[7] & 0xFF) | ((bytes[8] & 0xFF) << 8) | ((bytes[9] & 0xFF) << 16) | ((bytes[10] & 0xFF) << 24));
        }

        final String value = String.format("%d-%d-%d %d:%d:%d.%d", year, month, day, hours, minutes, seconds, nanos);
        if (isStructuredTemporalMode()) {
            return MySqlValueConverters.stringToBinlogDateTimeValue(value);
        }
        return MySqlValueConverters.containsZeroValuesInDatePart(value, column, table)
                ? null
                : rs.getObject(columnIndex, LocalDateTime.class);
    }
}
