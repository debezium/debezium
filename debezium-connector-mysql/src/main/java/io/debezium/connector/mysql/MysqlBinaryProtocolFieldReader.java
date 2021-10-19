/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.protocol.a.NativeConstants;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Decode binary protocol value for MySQL.
 *
 * Consult <a href="https://dev.mysql.com/doc/internals/en/binary-protocol-value.html">Binary Protocol Value</a> if you want to learn more about this.
 *
 * @author yangjie
 */
public class MysqlBinaryProtocolFieldReader extends AbstractMysqlFieldReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBinaryProtocolFieldReader.class);

    /**
     * @see <a href="https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_TIME">ProtocolBinary::MYSQL_TYPE_TIME</a>
     */
    @Override
    protected Object readTimeField(ResultSet rs, int columnIndex) throws SQLException {
        Blob b = rs.getBlob(columnIndex);
        if (b == null) {
            return null; // Don't continue parsing time field if it is null
        }
        else if (b.length() == 0) {
            LOGGER.warn("Encountered a zero length blob for column index {}", columnIndex);
            return null;
        }

        // if micro_seconds is 0, length is 8; otherwise length is 12
        if (b.length() != NativeConstants.BIN_LEN_TIME_NO_FRAC && b.length() != NativeConstants.BIN_LEN_TIME_WITH_MICROS) {
            throw new RuntimeException(String.format("Invalid length when read MySQL TIME value. BIN_LEN_TIME is %d", b.length()));
        }

        final byte[] bytes = b.getBytes(1, (int) (b.length()));
        final int days = (bytes[1] & 0xFF) | ((bytes[2] & 0xFF) << 8) | ((bytes[3] & 0xFF) << 16) | ((bytes[4] & 0xFF) << 24);
        final int hours = bytes[5];
        final int minutes = bytes[6];
        final int seconds = bytes[7];
        final int nanos = 1000 * days;
        final int finalHours = (bytes[0] == 1 ? days * -1 : days) * 24 + hours;

        return MySqlValueConverters.stringToDuration(String.format("%d:%d:%d.%d", finalHours, minutes, seconds, nanos));
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
        // length is 4
        if (b.length() != NativeConstants.BIN_LEN_DATE) {
            throw new RuntimeException(String.format("Invalid length when read MySQL DATE value. BIN_LEN_DATE is %d", b.length()));
        }

        final byte[] bytes = b.getBytes(1L, (int) b.length());
        final int year = (bytes[0] & 0xFF) | ((bytes[1] & 0xFF) << 8);
        final int month = bytes[2];
        final int day = bytes[3];

        return MySqlValueConverters.stringToLocalDate(String.format("%d-%d-%d", year, month, day), column, table);
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
            return null;
        }

        // if hour, minutes, seconds and micro_seconds are all 0, length is 4; if micro_seconds is 0, length is 7; otherwise length is 11
        if (b.length() != NativeConstants.BIN_LEN_DATE && b.length() != NativeConstants.BIN_LEN_TIMESTAMP_NO_FRAC
                && b.length() != NativeConstants.BIN_LEN_TIMESTAMP_WITH_MICROS) {
            throw new RuntimeException(String.format("Invalid length when read MySQL DATETIME value. BIN_LEN_DATETIME is %d", b.length()));
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

        return MySqlValueConverters.containsZeroValuesInDatePart(String.format("%d-%d-%d %d:%d:%d.%d", year, month, day, hours, minutes, seconds, nanos), column, table)
                ? null
                : rs.getTimestamp(columnIndex, Calendar.getInstance());
    }
}
