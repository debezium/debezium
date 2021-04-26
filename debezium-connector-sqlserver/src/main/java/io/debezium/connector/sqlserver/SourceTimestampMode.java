/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;

import io.debezium.config.EnumeratedValue;
import io.debezium.util.Clock;

/**
 * Strategy for populating the source.ts_ms field in change events.
 */
public enum SourceTimestampMode implements EnumeratedValue {

    /**
     * This mode (default) will set the source timestamp field (ts_ms) of when the record was committed in the database.
     */
    COMMIT("commit") {
        @Override
        protected Instant getTimestamp(SqlServerConnection connection, ResultSet resultSet, Clock clock) throws SQLException {
            return connection.normalize(resultSet.getTimestamp(resultSet.getMetaData().getColumnCount()));
        }

        /**
         * Returns the query for obtaining the LSN-to-TIMESTAMP query. On SQL Server
         * 2016 and newer, the query will normalize the value to UTC. This means that
         * the {@link SqlServerConnection#SERVER_TIMEZONE_PROP_NAME} is not necessary to be given. The
         * returned TIMESTAMP will be adjusted by the JDBC driver using this VM's TZ (as
         * required by the JDBC spec), and that same TZ will be applied when converting
         * the TIMESTAMP value into an {@code Instant}.
         */
        @Override
        protected String lsnTimestampSelectStatement(boolean supportsAtTimeZone) {
            String result = ", " + SqlServerConnection.LSN_TIMESTAMP_SELECT_STATEMENT;
            if (supportsAtTimeZone) {
                result += " " + SqlServerConnection.AT_TIME_ZONE_UTC;
            }
            return result;
        }
    },

    /**
     * This mode will set the source timestamp field (ts_ms) of when the record was processed by Debezium.
     */
    PROCESSING("processing") {
        @Override
        protected Instant getTimestamp(SqlServerConnection connection, ResultSet resultSet, Clock clock) {
            return clock.currentTime();
        }

        @Override
        protected String lsnTimestampSelectStatement(boolean supportsAtTimeZone) {
            return "";
        }
    };

    private final String value;

    SourceTimestampMode(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    protected abstract Instant getTimestamp(SqlServerConnection connection, ResultSet resultSet, Clock clock) throws SQLException;

    protected abstract String lsnTimestampSelectStatement(boolean supportsAtTimeZone);

    public static SourceTimestampMode getDefaultMode() {
        return COMMIT;
    }

    static SourceTimestampMode fromMode(String mode) {
        return Arrays.stream(SourceTimestampMode.values())
                .filter(s -> s.name().equalsIgnoreCase(mode))
                .findFirst()
                .orElseGet(SourceTimestampMode::getDefaultMode);
    }
}
