/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;

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
        return rs.getTime(columnIndex);
    }

    @Override
    protected Object readDateField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        return rs.getDate(columnIndex);
    }

    @Override
    protected Object readTimestampField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        return rs.getTimestamp(columnIndex);
    }

}
