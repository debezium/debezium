/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Decode text protocol value for MySQL.
 *
 * @author yangjie
 */
public class MysqlTextProtocolFieldReader extends AbstractMysqlFieldReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlTextProtocolFieldReader.class);

    /**
     * As MySQL connector/J implementation is broken for MySQL type "TIME" we have to use a binary-ish workaround
     *
     * @link https://issues.jboss.org/browse/DBZ-342
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

        try {
            return MySqlValueConverters.stringToDuration(new String(b.getBytes(1, (int) (b.length())), "UTF-8"));
        }
        catch (UnsupportedEncodingException e) {
            logger.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }

    /**
     * In non-string mode the date field can contain zero in any of the date part which we need to handle as all-zero
     *
     */
    @Override
    protected Object readDateField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        Blob b = rs.getBlob(columnIndex);
        if (b == null) {
            return null; // Don't continue parsing date field if it is null
        }

        try {
            return MySqlValueConverters.stringToLocalDate(new String(b.getBytes(1, (int) (b.length())), "UTF-8"), column, table);
        }
        catch (UnsupportedEncodingException e) {
            logger.error("Could not read MySQL DATE value as UTF-8");
            throw new RuntimeException(e);
        }
    }

    /**
     * In non-string mode the time field can contain zero in any of the date part which we need to handle as all-zero
     *
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

        try {
            return MySqlValueConverters.containsZeroValuesInDatePart((new String(b.getBytes(1, (int) (b.length())), "UTF-8")), column, table) ? null
                    : rs.getTimestamp(columnIndex, Calendar.getInstance());
        }
        catch (UnsupportedEncodingException e) {
            logger.error("Could not read MySQL DATETIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }
}
