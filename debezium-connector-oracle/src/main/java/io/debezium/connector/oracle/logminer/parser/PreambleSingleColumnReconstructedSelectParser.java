/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import java.util.function.Consumer;

import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.relational.Table;
import io.debezium.text.ParsingException;
import io.debezium.util.Strings;

/**
 * An abstract implementation that knows how to parse a basic Oracle SELECT statement
 * that contains a single column with one or more where conditions.
 *
 * @author Chris Cranford
 */
public abstract class PreambleSingleColumnReconstructedSelectParser {

    private static final String SELECT = "select";
    private static final String INTO = "into";
    private static final String FROM = "from";
    private static final String WHERE = "where";
    private static final String AND = "and";
    private static final String OR = "or";
    private static final String FOR_UPDATE = "for update";

    private final String preamble;

    private String columnName;
    private String schemaName;
    private String tableName;
    private Object[] columnValues;

    public PreambleSingleColumnReconstructedSelectParser(String preamble) {
        this.preamble = preamble;
    }

    public LogMinerDmlEntry parse(String sql, Table table) {
        // Reset internal state
        reset(table);

        if (!Strings.isNullOrBlank(sql)) {
            try {
                int startIndex = sql.indexOf(preamble);
                if (startIndex == -1) {
                    throw new IllegalStateException("Failed to locate preamble: " + preamble);
                }
                startIndex = sql.indexOf(" ", startIndex + preamble.length() + 1);
                startIndex = sql.indexOf(SELECT, startIndex);
                if (startIndex == -1) {
                    throw new IllegalStateException("Failed to locate SELECT keyword");
                }
                startIndex = sql.indexOf(" ", startIndex) + 1;
                startIndex = parseSelectable(sql, startIndex, table);
                startIndex = sql.indexOf(" ", startIndex) + 1; // skip leading space
                if (startsWithAtIndex(INTO, startIndex, sql)) {
                    // INTO detected
                    startIndex += INTO.length() + 1;
                    startIndex = parseIntoClause(sql, startIndex);
                }
                if (startsWithAtIndex(FROM, startIndex, sql)) {
                    // FROM detected
                    startIndex += FROM.length() + 1;
                    startIndex = parseFromClause(sql, startIndex);
                }
                if (startsWithAtIndex(WHERE, startIndex, sql)) {
                    // WHERE detected
                    startIndex += WHERE.length() + 1;
                    startIndex = parseWhereClause(sql, startIndex, table);
                }

                ParserUtils.setColumnUnavailableValues(columnValues, table);
            }
            catch (Throwable t) {
                throw new ParsingException(null, "Parsing failed for SQL: '" + sql + "'", t);
            }
        }

        final LogMinerDmlEntry entry = createDmlEntryForColumnValues(columnValues);
        entry.setObjectOwner(schemaName);
        entry.setObjectName(tableName);
        return entry;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    protected int parseQuotedValue(String sql, int index, Consumer<String> collector) {
        boolean inDoubleQuotes = false;
        int startIndex = -1, lastIndex = -1;
        for (int i = index; i < sql.length(); ++i) {
            if (sql.charAt(i) == '"') {
                if (!inDoubleQuotes) {
                    inDoubleQuotes = true;
                    startIndex = i + 1;
                }
                else {
                    inDoubleQuotes = false;
                    lastIndex = i;
                    index = i + 1;
                    break;
                }
            }
        }

        if (startIndex != -1 && lastIndex != -1) {
            collector.accept(sql.substring(startIndex, lastIndex));
        }

        return index;
    }

    protected int parseSelectable(String sql, int index, Table table) {
        return parseQuotedValue(sql, index, value -> {
            if (table.columnWithName(value) == null) {
                throw new IllegalStateException("No column named " + value + " found in table " + table.id());
            }
            columnName = value;
        });
    }

    protected int parseIntoClause(String sql, int index) {
        // By default, skipped
        return index;
    }

    protected int parseFromClause(String sql, int index) {
        index = parseQuotedValue(sql, index, value -> schemaName = value);
        if (sql.charAt(index) != '.') {
            throw new IllegalStateException("Expected object with format \"<schema>\".\"<table>\"");
        }

        index += 1; // skip dot
        index = parseQuotedValue(sql, index, value -> tableName = value);
        index += 1; // space

        return index;
    }

    protected int parseWhereClause(String sql, int index, Table table) {
        for (int i = index; i < sql.length(); ++i) {
            // parse column name
            StringBuilder columnName = new StringBuilder();
            index = parseQuotedValue(sql, index, columnName::append);
            index = parseOperator(sql, index);
            index = parseColumnValue(sql, index, LogMinerHelper.getColumnIndexByName(columnName.toString(), table));
            index += 1; // space

            if (sql.indexOf(AND, index) == index) {
                // 'and' detected
                index += AND.length() + 1;
            }
            else if (sql.indexOf(OR, index) == index) {
                // 'or' detecteed
                index += OR.length() + 1;
            }
            else if (sql.indexOf(FOR_UPDATE, index) == index) {
                // 'for update' detected
                // this signifies the end of the where clause
                break;
            }
            else if (index >= sql.length()) {
                // end of SQL
                break;
            }
        }
        return index;
    }

    protected int parseOperator(String sql, int index) {
        boolean initialSpace = false;
        for (int i = index; i < sql.length(); ++i) {
            char c = sql.charAt(i);
            char lookAhead = (i + 1 < sql.length()) ? sql.charAt(i + 1) : 0;
            if (!initialSpace && c == ' ') {
                initialSpace = true;
            }
            else if (initialSpace && c == '=' && lookAhead == ' ') {
                // equals operator
                index += 3;
                break;
            }
            else if (initialSpace && c == 'I' && lookAhead == 'S') {
                char lookAhead2 = (i + 2 < sql.length()) ? sql.charAt(i + 2) : 0;
                if (lookAhead2 == ' ') {
                    index += 4;
                    break;
                }
                throw new ParsingException(null, "Expected 'IS' at index " + i + ": " + sql);
            }
            else {
                throw new ParsingException(null, "Failed to parse operator at index " + i + ": " + sql);
            }
        }
        return index;
    }

    protected int parseColumnValue(String sql, int index, int columnIndex) {
        boolean inSingleQuotes = false;
        int start = -1, last = -1, nested = 0;
        for (int i = index; i < sql.length(); ++i) {
            char c = sql.charAt(i);
            char lookAhead = (i + 1 < sql.length()) ? sql.charAt(i + 1) : 0;
            if (i == index && c != '\'') {
                start = i;
            }
            else if (c == '(' && !inSingleQuotes) {
                nested++;
            }
            else if (c == ')' && !inSingleQuotes) {
                nested--;
                if (nested == 0) {
                    last = i + 1;
                    index = i + 1;
                    break;
                }
            }
            else if (c == '\'') {
                // skip over double single quote
                if (inSingleQuotes && lookAhead == '\'') {
                    i += 1;
                    continue;
                }
                if (inSingleQuotes) {
                    inSingleQuotes = false;
                    if (nested == 0) {
                        last = i;
                        index = i + 1;
                        break;
                    }
                    continue;
                }
                inSingleQuotes = true;
                if (nested == 0) {
                    start = i + 1;
                }
            }
            else if (c == ' ' && !inSingleQuotes && nested == 0) {
                last = i;
                index = i;
                break;
            }
        }

        if (start != -1) {
            final String value;
            if (last != -1) {
                // Index was already set above
                value = sql.substring(start, last);
            }
            else {
                // Index hasn't been set since we reached end of the SQL
                value = sql.substring(start);
                index = sql.length();
            }
            if (!value.equalsIgnoreCase("null")) {
                columnValues[columnIndex] = value;
            }
        }

        return index;
    }

    protected void reset(Table table) {
        this.columnName = null;
        this.schemaName = null;
        this.tableName = null;
        this.columnValues = new Object[table.columns().size()];
    }

    protected abstract LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues);

    private boolean startsWithAtIndex(String startsWithValue, int index, String value) {
        for (int i = 0; i < startsWithValue.length(); ++i) {
            if (value.charAt(index++) != startsWithValue.charAt(i)) {
                return false;
            }
        }
        return true;
    }

}
