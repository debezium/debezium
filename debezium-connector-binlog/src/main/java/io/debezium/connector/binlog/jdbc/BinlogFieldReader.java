/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.jdbc;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

/**
 * Decodes a binlog connector's JDBC result set return value based on configured protocols.
 *
 * @author yangjie
 * @author Chris Cranford
 */
public abstract class BinlogFieldReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogFieldReader.class);
    private static final Set<String> TEXT_DATA_TYPES = Collect.unmodifiableSet("CHAR", "VARCHAR", "TEXT");

    private final BinlogConnectorConfig connectorConfig;

    public BinlogFieldReader(BinlogConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    /**
     * Read the field from the provided JDBC {@link ResultSet}.
     *
     * @param rs the result set to be read, should not be null
     * @param columnIndex the column index or position to read
     * @param column the relational model column being read, never null
     * @param table the relational table being read, never null
     * @return the field's value
     * @throws SQLException if there is a database exception or failure reading the column's value
     */
    public Object readField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        switch (column.jdbcType()) {
            case Types.TIME:
                return readTimeField(rs, columnIndex, column, table);

            case Types.DATE: {
                try {
                    return readDateField(rs, columnIndex, column, table);
                }
                catch (RuntimeException e) {
                    LOGGER.warn("Failed to read data value: '{}'. Trying default implementation.", e.getMessage());
                    // Workaround for DBZ-5084
                    return rs.getObject(columnIndex);
                }
            }

            // This is for DATETIME columns (a logical date + time without time zone).
            // Reading them with a calendar based on the default time zone, we make sure that the value
            // is constructed correctly using the database's (or connection's) time zone.
            case Types.TIMESTAMP: {
                try {
                    return readTimestampField(rs, columnIndex, column, table);
                }
                catch (RuntimeException e) {
                    LOGGER.warn("Failed to read data value: '{}'. Trying default implementation.", e.getMessage());
                    // Workaround for DBZ-5084
                    return rs.getObject(columnIndex);
                }
            }

            // JDBC getObject returns a Boolean for all TINYINT(1) columns.
            // TINYINT columns are reported as SMALLINT by the JDBC driver.
            case Types.TINYINT:
            case Types.SMALLINT:
                // rs.wasNull() returns false when default value is set and NULL is inserted
                // For this, getObject() is necessary to identify if the value was provided
                // If there is a value, the read it
                return rs.getObject(columnIndex) == null ? null : rs.getInt(columnIndex);
        }

        // DBZ-2673
        // Check the type names as types like ENUM and SET are reported as JDBC char types
        if (hasValueConverter(column, table.id()) && TEXT_DATA_TYPES.contains(column.typeName())) {
            try {
                String data = rs.getString(columnIndex);
                if (data != null) {
                    return data.getBytes(getCharacterSet(column));
                }
            }
            catch (UnsupportedEncodingException e) {
                LOGGER.warn("Unsupported encoding '{}' for column '{}', sending value as String", e.getMessage(), column.name());
            }
        }

        // Fallback
        return rs.getObject(columnIndex);
    }

    /**
     * Reads the time field based on the requirements of the concrete field reader.*
     */
    protected abstract Object readTimeField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException;

    /**
     * Reads the date field based on the requirements of the concrete field reader.*
     */
    protected abstract Object readDateField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException;

    /**
     * Reads the timestamp field based on the requirements of the concrete field reader.*
     */
    protected abstract Object readTimestampField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException;

    /**
     * Read the column's character set.
     */
    protected abstract String getCharacterSet(Column column) throws UnsupportedEncodingException;

    /**
     * Common handler for logging invalid values.
     *
     * @param rs the result set, should not be null
     * @param columnIndex the column index or position read
     * @param value the value that was read
     * @throws SQLException if there is a database exception trying to get column metadata information
     */
    protected void logInvalidValue(ResultSet rs, int columnIndex, Object value) throws SQLException {
        final String columnName = rs.getMetaData().getColumnName(columnIndex);
        LOGGER.trace("Column '{}', detected an invalid value of '{}'.", columnName, value);
    }

    protected boolean hasValueConverter(Column column, TableId tableId) {
        return connectorConfig.customConverterRegistry().getValueConverter(tableId, column).isPresent();
    }
}
