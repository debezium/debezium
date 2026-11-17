/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;

/**
 * Parser for decoding a {@code 32K_WRITE} LogMiner REDO_SQL value.
 * <p>
 * The statement assigns the value to a buffer and appends the buffer to a temporary LOB:
 *
 * <pre>
 *     buf_c := 'value';   DBMS_LOB.WriteAppend(TempLob, LENGTH(buf_c), buf_c);
 *     buf_nc := ''value'';   DBMS_LOB.WriteAppend(TempLob, LENGTH(buf_nc), buf_nc);
 * </pre>
 *
 * The first form is used for {@code VARCHAR2} columns, the second for {@code NVARCHAR2} columns. Single
 * quotes that are part of the value are not escaped in either form.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class ExtendedStringWriteParser {

    private static final String ASSIGNMENT = ":=";
    private static final String NATIONAL_BUFFER = "buf_nc";

    private ExtendedStringWriteParser() {
    }

    /**
     * Parses the {@code 32K_WRITE} redo SQL.
     *
     * @param redoSql the SQL to be parsed, should not be {@code null}
     * @return the value that is written, never {@code null}
     */
    public static String parse(String redoSql) {
        // The statement ends with the append of the buffer, and the assignment's semicolon precedes it
        int endIndex = redoSql.lastIndexOf(';');
        if (endIndex != -1) {
            endIndex = redoSql.lastIndexOf(';', endIndex - 1);
        }

        final int assignmentIndex = redoSql.indexOf(ASSIGNMENT);
        if (endIndex == -1 || assignmentIndex == -1 || assignmentIndex > endIndex) {
            throw new DebeziumException("Unable to parse unsupported 32K_WRITE SQL: " + redoSql);
        }

        // A national value is enclosed in two single quotes on each side, any other value in one
        final int quotes = NATIONAL_BUFFER.equals(redoSql.substring(0, assignmentIndex).trim()) ? 2 : 1;

        final int quoteIndex = redoSql.indexOf('\'', assignmentIndex);
        final int startIndex = quoteIndex + quotes;
        final int valueEndIndex = endIndex - quotes;
        if (quoteIndex == -1 || valueEndIndex < startIndex) {
            throw new DebeziumException("Unable to parse unsupported 32K_WRITE SQL: " + redoSql);
        }

        return redoSql.substring(startIndex, valueEndIndex);
    }
}
