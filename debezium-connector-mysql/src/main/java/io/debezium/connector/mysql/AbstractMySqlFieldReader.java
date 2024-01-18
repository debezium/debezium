/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.Collect;

/**
 * Abstract class for decode MySQL return value according to different protocols.
 *
 * @author yangjie
 */
public abstract class AbstractMySqlFieldReader implements MySqlFieldReader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private static final Set<String> TEXT_DATATYPES = Collect.unmodifiableSet("CHAR", "VARCHAR", "TEXT");

    private final MySqlConnectorConfig connectorConfig;

    protected AbstractMySqlFieldReader(MySqlConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public Object readField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        if (column.jdbcType() == Types.TIME) {
            return readTimeField(rs, columnIndex);
        }

        if (column.jdbcType() == Types.DATE) {
            try {
                return readDateField(rs, columnIndex, column, table);
            }
            catch (RuntimeException e) {
                logger.warn("Failed to read date value: '{}'. Trying default ResultSet implementation.", e.getMessage());
                // If our field reader failed, let's try JDBC as the last resort.
                // Workaround for DBZ-5084.
                return rs.getObject(columnIndex);
            }
        }

        // This is for DATETIME columns (a logical date + time without time zone)
        // by reading them with a calendar based on the default time zone, we make sure that the value
        // is constructed correctly using the database's (or connection's) time zone
        if (column.jdbcType() == Types.TIMESTAMP) {
            try {
                return readTimestampField(rs, columnIndex, column, table);
            }
            catch (RuntimeException e) {
                logger.warn("Failed to read timestamp value: '{}'. Trying default ResultSet implementation.", e.getMessage());
                // If our field reader failed, let's try JDBC as the last resort.
                // Workaround for DBZ-5084.
                return rs.getObject(columnIndex);
            }
        }

        // JDBC's rs.GetObject() will return a Boolean for all TINYINT(1) columns.
        // TINYINT columns are reported as SMALLINT by JDBC driver
        if (column.jdbcType() == Types.TINYINT || column.jdbcType() == Types.SMALLINT) {
            // It seems that rs.wasNull() returns false when default value is set and NULL is inserted
            // We thus need to use getObject() to identify if the value was provided and if yes then
            // read it again to get correct scale
            return rs.getObject(columnIndex) == null ? null : rs.getInt(columnIndex);
        }

        // DBZ-2673
        // It is necessary to check the type names as types like ENUM and SET are
        // also reported as JDBC type char
        boolean hasConverterForColumn = connectorConfig.customConverterRegistry()
                .getValueConverter(table.id(), column)
                .isPresent();
        if (hasConverterForColumn && TEXT_DATATYPES.contains(column.typeName())) {
            try {
                String columnData = rs.getString(columnIndex);
                if (columnData != null) {
                    return columnData.getBytes(connectorConfig.getConnectorAdapter().getJavaEncodingForCharSet(column.charsetName()));
                }
            }
            catch (UnsupportedEncodingException e) {
                logger.warn("Unsupported encoding '{}' for column '{}', sending value as String", e.getMessage(), column.name());
            }
        }

        return rs.getObject(columnIndex);
    }

    protected abstract Object readTimeField(ResultSet rs, int columnIndex) throws SQLException;

    protected abstract Object readDateField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException;

    protected abstract Object readTimestampField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException;

    protected void logInvalidValue(ResultSet resultSet, int columnIndex, Object value) throws SQLException {
        final String columnName = resultSet.getMetaData().getColumnName(columnIndex);
        logger.trace("Column '" + columnName + "', detected an invalid value of '" + value + "'");
    }
}
