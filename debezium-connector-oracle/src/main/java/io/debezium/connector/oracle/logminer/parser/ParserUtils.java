/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Utility helper methods for the Oracle LogMiner DML parsing classes.
 *
 * @author Chris Cranford
 */
public class ParserUtils {

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
     * If the column value is {@code null} and the column is an LOB or XML-based column, this method will
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
        if (value == null && OracleDatabaseSchema.isNullReplacedByUnavailableValue(column)) {
            return OracleValueConverters.UNAVAILABLE_VALUE;
        }
        return value;
    }
}
