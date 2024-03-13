/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

import oracle.sql.TIMESTAMP;

/**
 * Utility helper methods for the Oracle LogMiner DML parsing classes.
 *
 * @author Chris Cranford
 */
public class ParserUtils {

    private static final int SIZE_TIMESTAMP_LTZ = 11;

    /**
     * Set the unavailable value placeholder in the column value array for null LOB-based columns.
     *
     * @param columnValues the column values array, should not be {@code null}
     * @param table the relational model table, should not be {@code null}
     */
    public static void setColumnUnavailableValues(Object[] columnValues, Table table) {
        for (int i = 0; i < columnValues.length; ++i) {
            // set unavailable value in the object array if applicable
            columnValues[i] = getColumnUnavailableValue(columnValues[i], table.columns().get(i));
        }
    }

    /**
     * Resolve the column value for a given column value and column instance.
     * <p>
     * If the column value is {@code null} and the column is an LOB-based column, this method will
     * resolve the final column value as {@link OracleValueConverters#UNAVAILABLE_VALUE}, a value
     * that represents that the column should be emitted with the unavailable value placeholder.
     * <p>
     * If the column value is not {@code null} or is not an LOB-based column, the method will
     * simply return the column's value as-is without modification.
     *
     * @param value the column's value, may be {@code null}
     * @param column the relational model's column instance, should not be {@code null}
     * @return the resolved column's value
     */
    public static Object getColumnUnavailableValue(Object value, Column column) {
        if (value == null && OracleDatabaseSchema.isLobColumn(column)) {
            return OracleValueConverters.UNAVAILABLE_VALUE;
        }
        return value;
    }

    public static ZonedDateTime toZonedDateTime(byte[] dataBytes, ZoneId databaseZoneId, ZoneId sessionZoneId) {
        int dataLength = dataBytes.length;
        int[] dataArray = new int[dataLength];

        int i;
        for(i = 0; i < dataLength; ++i) {
            dataArray[i] = dataBytes[i] & 255;
        }

        i = 0;
        if (dataLength == SIZE_TIMESTAMP_LTZ) {
            i = TIMESTAMP.getNanos(dataBytes, 7);
        }

        int year = TIMESTAMP.getJavaYear(dataArray[0], dataArray[1]);
        ZonedDateTime zonedDateTime = ZonedDateTime.of(year, dataArray[2], dataArray[3], dataArray[4] - 1, dataArray[5] - 1, dataArray[6] - 1, i, databaseZoneId);
        return zonedDateTime.withZoneSameInstant(sessionZoneId);
    }
}
