/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;

import org.hibernate.cfg.AvailableSettings;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.junit.jupiter.MySqlSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;

/**
 * Implementation of the JDBC sink connector multi-source pipeline that writes to MySQL.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("e2e")
@Tag("e2e-mysql")
@ExtendWith(MySqlSinkDatabaseContextProvider.class)
public class JdbcSinkPipelineToMySqlIT extends AbstractJdbcSinkPipelineIT {

    @Override
    protected String getBooleanType() {
        return "BIT";
    }

    @Override
    protected String getBitsDataType() {
        return "BIT";
    }

    @Override
    protected String getInt8Type() {
        return "TINYINT";
    }

    @Override
    protected String getInt16Type() {
        return "SMALLINT";
    }

    @Override
    protected String getInt32Type() {
        return "INT";
    }

    @Override
    protected String getInt64Type() {
        return "BIGINT";
    }

    @Override
    protected String getVariableScaleDecimalType() {
        return "DOUBLE";
    }

    @Override
    protected String getDecimalType() {
        return "DECIMAL";
    }

    @Override
    protected String getFloat32Type() {
        return "FLOAT";
    }

    @Override
    protected String getFloat64Type() {
        return "DOUBLE";
    }

    @Override
    protected String getCharType(Source source, boolean key, boolean nationalized) {
        if (!source.getOptions().isColumnTypePropagated() && !key) {
            return "LONGTEXT";
        }
        else if (!key) {
            return "CHAR";
        }
        return "VARCHAR";
    }

    @Override
    protected String getStringType(Source source, boolean key, boolean nationalized, boolean maxLength) {
        if (maxLength) {
            return getTextType(nationalized);
        }
        // Debezium does not propagate column type details for keys.
        if (!source.getOptions().isColumnTypePropagated() && !key) {
            return "LONGTEXT";
        }
        return "VARCHAR";
    }

    @Override
    protected String getTextType(boolean nationalized) {
        return "LONGTEXT";
    }

    @Override
    protected String getBinaryType(Source source, String sourceDataType) {
        if (source.getOptions().isColumnTypePropagated()) {
            if ("TINYBLOB".equalsIgnoreCase(sourceDataType)) {
                return "TINYBLOB";
            }
            else if ("MEDIUMBLOB".equalsIgnoreCase(sourceDataType)) {
                return "MEDIUMBLOB";
            }
            else if ("BLOB".equalsIgnoreCase(sourceDataType)) {
                return "BLOB";
            }
            else if ("LONGBLOB".equalsIgnoreCase(sourceDataType)) {
                return "LONGBLOB";
            }
            return "VARBINARY";
        }
        return "LONGBLOB";
    }

    @Override
    protected String getJsonType(Source source) {
        return "JSON";
    }

    @Override
    protected String getXmlType(Source source) {
        return "LONGTEXT";
    }

    @Override
    protected String getUuidType(Source source, boolean key) {
        return !key ? "LONGTEXT" : getStringType(source, key, false);
    }

    @Override
    protected String getEnumType(Source source, boolean key) {
        return "ENUM";
    }

    @Override
    protected String getSetType(Source source, boolean key) {
        return "SET";
    }

    @Override
    protected String getYearType() {
        return "YEAR";
    }

    @Override
    protected String getDateType() {
        return "DATE";
    }

    @Override
    protected String getTimeType(Source source, boolean key, int precision) {
        return "TIME";
    }

    @Override
    protected String getTimeWithTimezoneType() {
        return "DATETIME";
    }

    @Override
    protected String getTimestampType(Source source, boolean key, int precision) {
        return "DATETIME";
    }

    @Override
    protected String getTimestampWithTimezoneType(Source source, boolean key, int precision) {
        return "TIMESTAMP";
    }

    @Override
    protected String getIntervalType(Source source, boolean numeric) {
        return numeric ? getInt64Type() : getStringType(source, false, false);
    }

    @Override
    protected Timestamp getTimestamp(ResultSet rs, int index) throws SQLException {
        if (isConnectionTimeZoneSet()) {
            // We need to deal with the adjustment of time if the "connectionTimeZone" setting is applied.
            return rs.getTimestamp(index, Calendar.getInstance(getCurrentSinkTimeZone()));
        }
        return super.getTimestamp(rs, index);
    }

    @Override
    protected ZonedDateTime getTimestampAsZonedDateTime(ResultSet rs, int index) throws SQLException {
        if (isConnectionTimeZoneSet()) {
            // We need to make some adjustments due to "connectionTimeZone" setting being applied.
            return getTimestamp(rs, index).toLocalDateTime()
                    .atZone(ZoneId.systemDefault())
                    .withZoneSameInstant(getCurrentSinkTimeZone().toZoneId());
        }
        return super.getTimestampAsZonedDateTime(rs, index);
    }

    @Override
    protected OffsetTime getTimeAsOffsetTime(ResultSet rs, int index) throws SQLException {
        if (isConnectionTimeZoneSet()) {
            return getTimestamp(rs, index).toLocalDateTime()
                    .atZone(ZoneId.systemDefault())
                    .withZoneSameInstant(getCurrentSinkTimeZone().toZoneId())
                    .toOffsetDateTime()
                    .toOffsetTime();
        }
        return super.getTimeAsOffsetTime(rs, index);
    }

    private boolean isConnectionTimeZoneSet() {
        return getCurrentSinkConfig().getHibernateConfiguration()
                .getProperty(AvailableSettings.URL)
                .contains("connectionTimeZone=");
    }
}
