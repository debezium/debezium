/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import java.util.function.Consumer;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.relational.Table;
import io.debezium.text.ParsingException;

/**
 * Simple text-based parser implementation for Oracle LogMiner SEL_LOB_LOCATOR Redo SQL.
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public class SelectLobParser {

    private static final String BEGIN = "BEGIN";
    private static final String SELECT = "select";
    private static final String FROM = "from";
    private static final String WHERE = "where";
    private static final String AND = "and";
    private static final String OR = "or";
    private static final String FOR_UPDATE = "for update";
    private static final String BLOB_LOCATOR = "loc_b";
    private static final String BLOB_BUFFER = "buf_b";

    private String columnName;
    private String schemaName;
    private String tableName;
    private boolean binary;
    private Object[] columnValues;

    /**
     * Parse the supplied SEL_LOB_LOCATOR event redo SQL statement.
     *
     * @param sql SQL statement expression to be parsed
     * @param table the relational table
     * @return instance of {@link LogMinerDmlEntry} for the parsed fragment.
     */
    public LogMinerDmlEntry parse(String sql, Table table) {
        // Reset internal state
        reset(table);

        if (sql != null) {
            try {
                int start = sql.indexOf(BEGIN);
                if (start != -1) {
                    start = sql.indexOf(" ", start + 1) + 1;
                    if (sql.indexOf(SELECT, start) == start) {
                        start = sql.indexOf(" ", start) + 1;
                        start = parseQuotedValue(sql, start, s -> columnName = s);
                        start = sql.indexOf(" ", start) + 1; // skip leading space
                        start = sql.indexOf(" ", start) + 1; // skip into
                        if (sql.indexOf(BLOB_LOCATOR, start) == start || sql.indexOf(BLOB_BUFFER, start) == start) {
                            binary = true;
                        }
                        start = sql.indexOf(" ", start) + 1; // skip loc_xxxx variable name
                        if (sql.indexOf(FROM, start) == start) {
                            start = sql.indexOf(" ", start) + 1;
                            start = parseQuotedValue(sql, start, s -> schemaName = s);
                            if (sql.indexOf('.', start) == start) {
                                start += 1; // dot
                                start = parseQuotedValue(sql, start, s -> tableName = s);
                                start += 1; // space
                                if (sql.indexOf(WHERE, start) == start) {
                                    start += WHERE.length() + 1;
                                    parseWhere(sql, start, table);
                                }
                            }
                        }

                        ParserUtils.setColumnUnavailableValues(columnValues, table);
                    }
                }
            }
            catch (Throwable t) {
                throw new ParsingException(null, "Parsing failed for SEL_LOB_LOCATOR sql: '" + sql + "'", t);
            }
        }

        LogMinerDmlEntry entry = LogMinerDmlEntryImpl.forLobLocator(columnValues);
        entry.setObjectOwner(schemaName);
        entry.setObjectName(tableName);
        return entry;
    }

    /**
     * @return the column name that the SEL_LOB_LOCATOR event is modifying
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * @return true if the column being modified is a BLOB; otherwise false indicates a CLOB data type.
     */
    public boolean isBinary() {
        return binary;
    }

    private int parseQuotedValue(String sql, int index, Consumer<String> collector) {
        boolean inDoubleQuotes = false;
        int start = -1, last = -1;
        for (int i = index; i < sql.length(); ++i) {
            if (sql.charAt(i) == '"') {
                if (!inDoubleQuotes) {
                    inDoubleQuotes = true;
                    start = i + 1;
                }
                else {
                    inDoubleQuotes = false;
                    last = i;
                    index = i + 1;
                    break;
                }
            }
        }

        if (start != -1 && last != -1) {
            collector.accept(sql.substring(start, last));
        }

        return index;
    }

    private int parseColumnValue(String sql, int index, int columnIndex) {
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

        if (start != -1 && last != -1) {
            final String value = sql.substring(start, last);
            if (!value.equalsIgnoreCase("null")) {
                columnValues[columnIndex] = value;
            }
        }

        return index;
    }

    private int parseWhere(String sql, int index, Table table) {
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
        }
        return index;
    }

    private int parseOperator(String sql, int index) {
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

    private void reset(Table table) {
        columnName = null;
        schemaName = null;
        tableName = null;
        binary = false;
        columnValues = new Object[table.columns().size()];
    }
}
