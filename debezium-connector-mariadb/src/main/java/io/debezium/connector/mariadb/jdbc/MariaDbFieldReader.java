/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Objects;

import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * A {@link BinlogFieldReader} implementation for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbFieldReader extends BinlogFieldReader {

    public MariaDbFieldReader(MariaDbConnectorConfig connectorConfig, BinlogCharsetRegistry binlogCharsetRegistry) {
        super(connectorConfig, binlogCharsetRegistry);
    }

    @Override
    protected String getCharacterSet(Column column) {
        return getCharsetRegistry().getJavaEncodingForCharSet(column.charsetName());
    }

    @Override
    protected Object readTimeField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        final String value = rs.getString(columnIndex);
        return Objects.isNull(value) ? null : MariaDbValueConverters.stringToDuration(value);
    }

    @Override
    protected Object readDateField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        final String value = rs.getString(columnIndex);
        return Objects.isNull(value) ? null : MariaDbValueConverters.stringToLocalDate(value, column, table);
    }

    @Override
    protected Object readTimestampField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        final String value = rs.getString(columnIndex);
        if (Objects.isNull(value)) {
            return null;
        }
        return MariaDbValueConverters.containsZeroValuesInDatePart(value, column, table)
                ? null
                : rs.getTimestamp(columnIndex, Calendar.getInstance());
    }
}
