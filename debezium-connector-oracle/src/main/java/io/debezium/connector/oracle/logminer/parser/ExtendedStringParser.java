/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.relational.Table;

/**
 * @author Chris Cranford
 */
public class ExtendedStringParser extends AbstractSingleColumnSqlRedoPreambleParser {

    private static final String STMT_PREFIX = " Stmt := '";
    private static final String STMT_SUFFIX = "';";
    private static final String UPDATE = "update ";
    private static final String SET = " set ";

    @Override
    protected void parseInternal(String sql, Table table) {
        // We want to trim as the end of the SQL often has newlines
        sql = sql.trim();

        int startIndex = 0;
        startsWithAtIndexThrow(DECLARE, startIndex, sql);

        startIndex = indexOfThrow(BEGIN, sql, startIndex);

        startIndex = indexOfThrow(STMT_PREFIX, sql, startIndex);
        startIndex += STMT_PREFIX.length();

        if (!sql.endsWith(STMT_SUFFIX)) {
            throw new IllegalStateException("Expected 32K_BEGIN to end with \"';\": '" + sql + "'");
        }

        // We know the boundary for the UPDATE statement.
        final String update = sql.substring(startIndex, sql.length() - STMT_SUFFIX.length()).replace("''", "'");

        startIndex = 0;
        if (startsWithAtIndexThrow(UPDATE, startIndex, update)) {
            startIndex += UPDATE.length();
        }

        // Parse schema and table
        startIndex = parseQuotedValue(update, startIndex, value -> this.tableName = value);
        startIndex = parseQuotedValue(update, startIndex, value -> this.schemaName = value);

        if (startsWithAtIndexThrow(SET, startIndex, update)) {
            startIndex += SET.length();
        }

        startIndex = parseQuotedValue(update, startIndex, value -> this.columnName = value);

        if (startsWithAtIndexThrow(" = ", startIndex, update)) {
            startIndex += 3;
        }

        // Expect a ":VAR" syntax
        if (startsWithAtIndexThrow(":", startIndex, update)) {
            startIndex = update.indexOf(" ", startIndex); // advance to the next space
            if (startsWithAtIndexThrow(" ", startIndex, update)) {
                startIndex += 1;
            }
        }

        if (startsWithAtIndexThrow(WHERE, startIndex, update)) {
            startIndex += WHERE.length();
        }

        parseWhereClause(update, startIndex, table);

        ParserUtils.setColumnUnavailableValues(columnValues, table);
    }

    @Override
    protected LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues) {
        return LogMinerDmlEntryImpl.forExtendedString(columnValues);
    }
}
