/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueImpl;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntryImpl;
import io.debezium.data.Envelope;

/**
 * @author Chris Cranford
 */
public class FastDmlParser {

    private static final String SINGLE_QUOTE = "'";
    private static final String NULL = "NULL";
    private static final String INSERT_INTO = "insert into ";
    private static final String UPDATE = "update ";
    private static final String DELETE = "delete ";
    private static final String AND = "and ";
    private static final String OR = "or ";
    private static final String SET = " set ";
    private static final String WHERE = " where ";
    private static final String VALUES = " values ";

    /**
     * Parse a DML SQL statement.
     *
     * @param sql the sql statement
     * @return the parsed DML entry record or {@code null} if the SQL was not parsed
     */
    public LogMinerDmlEntry parse(String sql) {
        if (sql.startsWith(INSERT_INTO)) {
            return parseInsert(sql);
        }
        else if (sql.startsWith(UPDATE)) {
            return parseUpdate(sql);
        }
        else if (sql.startsWith(DELETE)) {
            return parseDelete(sql);
        }
        return null;
    }

    /**
     * Parse an {@code INSERT} SQL statement.
     *
     * @param sql the sql statement
     * @return the parsed DML entry record or {@code null} if the SQL was not parsed
     */
    private LogMinerDmlEntry parseInsert(String sql) {
        // advance beyond "insert into "
        int index = 12;

        // parse table
        index = parseTableName(sql, index);

        // capture column names
        List<String> columnNames = new ArrayList<>();
        index = parseColumnListClause(sql, index, columnNames);

        // capture values
        List<String> columnValues = new ArrayList<>();
        index = parseColumnValuesClause(sql, index, columnValues);

        List<LogMinerColumnValue> newValues = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); ++i) {
            newValues.add(createColumnValue(columnNames.get(i), columnValues.get(i)));
        }

        return new LogMinerDmlEntryImpl(Envelope.Operation.CREATE, newValues, Collections.emptyList());
    }

    /**
     * Parse an {@code UPDATE} SQL statement.
     *
     * @param sql the sql statement
     * @return the parsed DML entry record or {@code null} if the SQL was not parsed
     */
    private LogMinerDmlEntry parseUpdate(String sql) {
        // advance beyond "update "
        int index = 7;

        // parse table
        index = parseTableName(sql, index);

        // parse set
        List<String> newColumnNames = new ArrayList<>();
        List<String> newColumnValues = new ArrayList<>();
        index = parseSetClause(sql, index, newColumnNames, newColumnValues);

        // parse where
        List<String> oldColumnNames = new ArrayList<>();
        List<String> oldColumnValues = new ArrayList<>();
        parseWhereClause(sql, index, oldColumnNames, oldColumnValues);

        List<LogMinerColumnValue> oldValues = new ArrayList<>();
        for (int i = 0; i < oldColumnNames.size(); ++i) {
            LogMinerColumnValue value = new LogMinerColumnValueImpl(oldColumnNames.get(i), 0);
            value.setColumnData(oldColumnValues.get(i));
            oldValues.add(value);
        }

        List<LogMinerColumnValue> newValues = new ArrayList<>();
        for (LogMinerColumnValue oldValue : oldValues) {
            boolean found = false;
            for (int j = 0; j < newColumnNames.size(); ++j) {
                if (newColumnNames.get(j).equals(oldValue.getColumnName())) {
                    newValues.add(createColumnValue(newColumnNames.get(j), newColumnValues.get(j)));
                    found = true;
                    break;
                }
            }
            if (!found) {
                newValues.add(oldValue);
            }
        }

        return new LogMinerDmlEntryImpl(Envelope.Operation.UPDATE, newValues, oldValues);
    }

    /**
     * Parses a SQL {@code DELETE} statement.
     *
     * @param sql the sql statement
     * @return the parsed DML entry record or {@code null} if the SQL was not parsed
     */
    private LogMinerDmlEntry parseDelete(String sql) {
        // advance beyond "delete from "
        int index = 12;

        // parse table
        index = parseTableName(sql, index);

        // parse where
        List<String> columnNames = new ArrayList<>();
        List<String> columnValues = new ArrayList<>();
        parseWhereClause(sql, index, columnNames, columnValues);

        List<LogMinerColumnValue> oldValues = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); ++i) {
            oldValues.add(createColumnValue(columnNames.get(i), columnValues.get(i)));
        }

        return new LogMinerDmlEntryImpl(Envelope.Operation.DELETE, Collections.emptyList(), oldValues);
    }

    /**
     * Parses a table-name in the SQL clause
     *
     * @param sql the sql statement
     * @param index the index into the sql statement to begin parsing
     * @return the index into the sql string where the table name ended
     */
    private int parseTableName(String sql, int index) {
        boolean inQuote = false;

        for (; index < sql.length(); ++index) {
            char c = sql.charAt(index);
            if (c == '"') {
                if (inQuote) {
                    inQuote = false;
                    continue;
                }
                inQuote = true;
            }
            else if (c == ' ' || c == '(' && !inQuote) {
                break;
            }
        }

        return index;
    }

    /**
     * Parse an {@code INSERT} statement's column-list clause.
     *
     * @param sql the sql statement
     * @param start the index into the sql statement to begin parsing
     * @param columnNames the list that will be populated with the column names
     * @return the index into the sql string where the column-list clause ended
     */
    private int parseColumnListClause(String sql, int start, List<String> columnNames) {
        int index = start;
        boolean inQuote = false;
        for (; index < sql.length(); ++index) {
            char c = sql.charAt(index);
            if (c == '(' && !inQuote) {
                start = index + 1;
            }
            else if (c == ')' && !inQuote) {
                index++;
                break;
            }
            else if (c == '"') {
                if (inQuote) {
                    inQuote = false;
                    columnNames.add(sql.substring(start + 1, index));
                    start = index + 2;
                    continue;
                }
                inQuote = true;
            }
        }
        return index;
    }

    /**
     * Parse an {@code INSERT} statement's column-values clause.
     *
     * @param sql the sql statement
     * @param start the index into the sql statement to begin parsing
     * @param columnValues the list of that will populated with the column values
     * @return the index into the sql string where the column-values clause ended
     */
    private int parseColumnValuesClause(String sql, int start, List<String> columnValues) {
        int index = start;
        int nested = 0;
        boolean inQuote = false;
        boolean inValues = false;

        // verify entering values-clause
        if (!sql.substring(index, index + 8).equals(VALUES)) {
            throw new DebeziumException("Failed to parse DML: " + sql);
        }
        index += VALUES.length();

        for (; index < sql.length(); ++index) {
            char c = sql.charAt(index);
            if (c == '(' && !inQuote && !inValues) {
                inValues = true;
                start = index + 1;
            }
            else if (c == '(' && !inQuote) {
                nested++;
            }
            else if (c == '\'') {
                if (inQuote) {
                    inQuote = false;
                    continue;
                }
                inQuote = true;
            }
            else if (!inQuote && (c == ',' || c == ')')) {
                if (c == ')' && nested != 0) {
                    nested--;
                    continue;
                }
                if (c == ',' && nested != 0) {
                    continue;
                }
                String s = sql.substring(start, index);
                if (s.startsWith("'") && s.endsWith("'")) {
                    // if the value is single-quoted at the start/end, clear the quotes.
                    s = s.substring(1, s.length() - 1);
                }
                columnValues.add(s);
                start = index + 1;
            }
        }

        return index;
    }

    /**
     * Parse an {@code UPDATE} statement's {@code SET} clause.
     *
     * @param sql the sql statement
     * @param start the index into the sql statement to begin parsing
     * @param columnNames the list of the changed column names that will be populated
     * @param columnValues the list of the changed column values that will be populated
     * @return the index into the sql string where the set-clause ended
     */
    private int parseSetClause(String sql, int start, List<String> columnNames, List<String> columnValues) {
        boolean inQuote = false;
        boolean inSingleQuote = false;
        boolean inColumnName = true;
        boolean inColumnValue = false;
        int nested = 0;

        // verify entering set-clause
        if (!sql.substring(start, start + 5).equals(SET)) {
            throw new DebeziumException("Failed to parse DML: " + sql);
        }
        start += SET.length();

        int index = start;
        for (; index < sql.length(); ++index) {
            char c = sql.charAt(index);
            if (c == '"') {
                // where clause column names are double-quoted
                if (inQuote) {
                    inQuote = false;
                    continue;
                }
                inQuote = true;
            }
            else if (c == '\'') {
                if (inSingleQuote) {
                    inSingleQuote = false;
                    continue;
                }
                inSingleQuote = true;
            }
            else if (c == '=' && !inQuote && inColumnName) {
                String s = sql.substring(start + 1, index - 2);
                start = index + 2;
                columnNames.add(s);
                inColumnValue = true;
                inColumnName = false;
                index++;
            }
            else if (c == '(' && !inQuote && !inSingleQuote && inColumnValue) {
                nested++;
            }
            else if (c == ')' && !inQuote && !inSingleQuote && inColumnValue && nested > 0) {
                nested--;
            }
            else if ((c == ',' || c == ' ') && !inQuote && !inSingleQuote && inColumnValue) {
                if (nested > 0) {
                    continue;
                }
                String s = sql.substring(start, index);
                if (s.startsWith("'") && s.endsWith("'")) {
                    s = s.substring(1, s.length() - 1);
                }
                inColumnValue = false;
                columnValues.add(s);
                if (c == ',') {
                    start = index + 2;
                    inColumnName = true;
                }
                else {
                    start = index;
                }
            }
            else if (!inQuote && !inColumnValue && sql.substring(index).startsWith("where ")) {
                index--;
                break;
            }
            else if (!inQuote && !inColumnName && sql.substring(index).startsWith("and ")) {
                index += 3;
                start = index + 1;
                inColumnName = true;
            }
            else if (!inQuote && !inColumnName && sql.substring(index).startsWith("or ")) {
                index += 2;
                start = index + 1;
                inColumnName = true;
            }
        }

        return index;
    }

    /**
     * Parses a {@code WHERE} clause populates the provided column names and values arrays.
     *
     * @param sql the sql statement
     * @param start the index into the sql statement to begin parsing
     * @param columnNames the column names parsed from the clause
     * @param columnValues the column values parsed from the clause
     * @return the index into the sql string to continue parsing
     */
    private int parseWhereClause(String sql, int start, List<String> columnNames, List<String> columnValues) {
        int nested = 0;
        boolean inColumnName = true;
        boolean inColumnValue = false;
        boolean inQuote = false;
        boolean inSingleQuote = false;

        // verify entering where-clause
        if (!sql.substring(start, start + 7).equals(WHERE)) {
            throw new DebeziumException("Failed to parse DML: " + sql);
        }
        start += WHERE.length();

        int index = start;
        for (; index < sql.length(); ++index) {
            char c = sql.charAt(index);
            if (c == '"') {
                // where clause column names are double-quoted
                if (inQuote) {
                    inQuote = false;
                    continue;
                }
                inQuote = true;
            }
            else if (c == '=' && !inQuote && inColumnName) {
                String s = sql.substring(start + 1, index - 2);
                start = index + 2;
                columnNames.add(s);
                inColumnValue = true;
                inColumnName = false;
                index++;
            }
            else if (c == '\'') {
                if (inSingleQuote) {
                    inSingleQuote = false;
                    continue;
                }
                inSingleQuote = true;
            }
            else if (c == '(' && !inQuote && !inSingleQuote && inColumnValue) {
                nested++;
            }
            else if (c == ')' && !inQuote && !inSingleQuote && inColumnValue && nested > 0) {
                nested--;
            }
            else if (c == ' ' && !inQuote && !inSingleQuote && inColumnValue) {
                if (nested > 0) {
                    continue;
                }
                columnValues.add(removeSingleQuotes(sql.substring(start, index)));
                inColumnValue = false;
                start = index;
            }
            else if (!inQuote && !inColumnName && sql.substring(index).startsWith(AND)) {
                index += 3;
                start = index + 1;
                inColumnName = true;
            }
            else if (!inQuote && !inColumnName && sql.substring(index).startsWith(OR)) {
                index += 2;
                start = index + 1;
                inColumnName = true;
            }
            else if (c == ';' && !inQuote && !inSingleQuote && inColumnValue) {
                columnValues.add(removeSingleQuotes(sql.substring(start, index)));
            }
        }

        return index;
    }

    /**
     * Remove {@code '} quotes from around the provided text if they exist; otherwise the value is returned as-is.
     *
     * @param text the text to remove single quotes
     * @return the text with single quotes removed
     */
    private static String removeSingleQuotes(String text) {
        if (text.startsWith(SINGLE_QUOTE) && text.endsWith(SINGLE_QUOTE)) {
            return text.substring(1, text.length() - 1);
        }
        return text;
    }

    /**
     * Helper method to create a {@link LogMinerColumnValue} from a column name/value pair.
     *
     * @param columnName the column name
     * @param columnValue the column value
     * @return the LogMiner column value object
     */
    private static LogMinerColumnValue createColumnValue(String columnName, String columnValue) {
        LogMinerColumnValue value = new LogMinerColumnValueImpl(columnName, 0);
        if (columnValue != null && !columnValue.equals(NULL)) {
            value.setColumnData(columnValue);
        }
        return value;
    }
}
