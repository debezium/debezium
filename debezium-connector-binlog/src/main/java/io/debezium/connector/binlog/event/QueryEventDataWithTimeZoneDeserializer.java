/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

/**
 * A deserializer of query events that also reads the time zone of the session from the status variables of the event.
 * MySQL and MariaDB interpret {@code TIMESTAMP} literals of DDL statements in that time zone.
 */
public class QueryEventDataWithTimeZoneDeserializer implements EventDataDeserializer<QueryEventData> {

    // Status variables of query events, as defined by Query_log_event of MySQL and MariaDB
    private static final int Q_FLAGS2_CODE = 0;
    private static final int Q_SQL_MODE_CODE = 1;
    private static final int Q_CATALOG_CODE = 2;
    private static final int Q_AUTO_INCREMENT = 3;
    private static final int Q_CHARSET_CODE = 4;
    private static final int Q_TIME_ZONE_CODE = 5;
    private static final int Q_CATALOG_NZ_CODE = 6;
    private static final int Q_LC_TIME_NAMES_CODE = 7;
    private static final int Q_CHARSET_DATABASE_CODE = 8;
    private static final int Q_TABLE_MAP_FOR_UPDATE_CODE = 9;
    private static final int Q_MASTER_DATA_WRITTEN_CODE = 10;

    @Override
    public QueryEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        final QueryEventDataWithTimeZone eventData = new QueryEventDataWithTimeZone();
        eventData.setThreadId(inputStream.readLong(4));
        eventData.setExecutionTime(inputStream.readLong(4));
        inputStream.skip(1); // length of the database name, which is zero-terminated
        eventData.setErrorCode(inputStream.readInteger(2));
        final int statusVariablesLength = inputStream.readInteger(2);
        eventData.setTimeZone(readTimeZone(inputStream.read(statusVariablesLength)));
        eventData.setDatabase(inputStream.readZeroTerminatedString());
        eventData.setSql(inputStream.readString(inputStream.available()));
        return eventData;
    }

    /**
     * Reads the time zone from the status variables of a query event. Only the status variables that can precede the
     * time zone are decoded, and an unknown one ends the search, as the length of its value is not known.
     *
     * @param statusVariables the status variables; never null
     * @return the time zone, or null if the status variables do not contain it
     */
    static String readTimeZone(byte[] statusVariables) {
        int position = 0;
        while (position < statusVariables.length) {
            final int code = statusVariables[position++] & 0xFF;
            switch (code) {
                case Q_FLAGS2_CODE, Q_MASTER_DATA_WRITTEN_CODE, Q_AUTO_INCREMENT -> position += 4;
                case Q_SQL_MODE_CODE, Q_TABLE_MAP_FOR_UPDATE_CODE -> position += 8;
                case Q_CHARSET_CODE -> position += 6;
                case Q_LC_TIME_NAMES_CODE, Q_CHARSET_DATABASE_CODE -> position += 2;
                case Q_CATALOG_CODE, Q_CATALOG_NZ_CODE, Q_TIME_ZONE_CODE -> {
                    // A length and a name, followed by a terminating zero for the old catalog variable
                    if (position >= statusVariables.length) {
                        return null;
                    }
                    final int length = statusVariables[position++] & 0xFF;
                    if (code == Q_TIME_ZONE_CODE) {
                        return position + length <= statusVariables.length
                                ? new String(statusVariables, position, length, StandardCharsets.US_ASCII)
                                : null;
                    }
                    position += length + (code == Q_CATALOG_CODE ? 1 : 0);
                }
                default -> {
                    return null;
                }
            }
        }
        return null;
    }
}
