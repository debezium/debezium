/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.DebeziumException;
import io.debezium.text.ParsingException;

/**
 * Extracts the written chunk from a {@code 32K_WRITE} redo statement.
 *
 * @author Chris Cranford
 */
public final class ExtendedStringWriteParser {

    private ExtendedStringWriteParser() {
    }

    /**
     * @param redoSql the {@code 32K_WRITE} redo statement, should not be {@code null}
     * @return the chunk of extended string data the statement writes
     * @throws ParsingException if the statement does not have the expected shape
     */
    public static String parse(String redoSql) {
        try {
            int endIndex = redoSql.lastIndexOf(";");
            if (endIndex == -1) {
                throw new DebeziumException("Failed to find end index on 32K_WRITE operation");
            }

            endIndex = redoSql.lastIndexOf(";", endIndex - 1);
            if (endIndex == -1) {
                throw new DebeziumException("Failed to find end index on 32K_WRITE operation");
            }

            return redoSql.substring(12, endIndex - 1);
        }
        catch (Exception e) {
            throw new ParsingException(null, "Failed to parse 32K_WRITE event", e);
        }
    }
}
