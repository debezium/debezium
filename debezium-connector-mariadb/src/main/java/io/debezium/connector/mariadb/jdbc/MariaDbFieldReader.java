/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.Objects;

import io.debezium.connector.binlog.event.BinlogDateTimeValue;
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

    public MariaDbFieldReader(MariaDbConnectorConfig connectorConfig) {
        super(connectorConfig);
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
        if (Objects.isNull(value)) {
            return null;
        }
        if (isStructuredTemporalMode()) {
            return MariaDbValueConverters.stringToBinlogDateValue(value);
        }
        return MariaDbValueConverters.stringToLocalDate(value, column, table);
    }

    @Override
    protected Object readTimestampField(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        final String value = rs.getString(columnIndex);
        if (Objects.isNull(value)) {
            return null;
        }
        // The driver formats a DATETIME string with the year of era and in the JVM's default time zone, which changes the
        // year zero and a value that does not exist in that time zone, so the value is read as a LocalDateTime instead
        if (isStructuredTemporalMode()) {
            try {
                final LocalDateTime dateTime = rs.getObject(columnIndex, LocalDateTime.class);
                if (dateTime != null) {
                    return new BinlogDateTimeValue(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                            dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(), dateTime.getNano());
                }
            }
            catch (DateTimeException e) {
                // Not a valid date
            }
            // The driver returns zero values and invalid dates as stored
            return MariaDbValueConverters.stringToBinlogDateTimeValue(value);
        }
        return MariaDbValueConverters.containsZeroValuesInDatePart(value, column, table)
                ? null
                : rs.getObject(columnIndex, LocalDateTime.class);
    }
}
