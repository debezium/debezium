/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * A simple protocol field handler for MariaDB.
 *
 * This field handler is only enabled when the user configuration specifically specifies the
 * {@code database.protocol} property as "jdbc:mariadb".
 *
 * @author Chris Cranford
 *
 * @see MySqlConnectorTask's method getFieldReader
 */
public class MariaDbProtocolFieldReader extends AbstractMySqlFieldReader {

    public MariaDbProtocolFieldReader(MySqlConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    protected Object readTimeField(ResultSet rs, int columnIndex) throws SQLException {
        final String value = rs.getString(columnIndex);
        if (value == null) {
            return null;
        }
        return MySqlValueConverters.stringToDuration(value);
    }

    @Override
    protected Object readDateField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        String value = rs.getString(columnIndex);
        if (value == null) {
            return value;
        }
        return MySqlValueConverters.stringToLocalDate(value, column, table);
    }

    @Override
    protected Object readTimestampField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        String value = rs.getString(columnIndex);
        if (value == null) {
            return value;
        }
        return MySqlValueConverters.containsZeroValuesInDatePart(value, column, table)
                ? null
                : rs.getTimestamp(columnIndex, Calendar.getInstance());
    }

}
