/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.logminer.RowMapper;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueImpl;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntryImpl;

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
    private List<LogMinerColumnValue> columns;

    /**
     * Parse the supplied SEL_LOB_LOCATOR event redo SQL statement.
     *
     * @param sql SQL statement expression to be parsed
     * @return instance of {@link LogMinerDmlEntry} for the parsed fragment.
     */
    public LogMinerDmlEntry parse(String sql) {
        // Reset internal state
        reset();

        if (sql != null) {
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
                                parseWhere(sql, start);
                            }
                        }
                    }
                }
            }
        }

        LogMinerDmlEntryImpl entry = new LogMinerDmlEntryImpl(RowMapper.SELECT_LOB_LOCATOR, new ArrayList<>(columns), columns);
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

    private int parseColumnValue(String sql, int index, Consumer<String> collector) {
        boolean inSingleQuotes = false;
        int start = -1, last = -1;
        for (int i = index; i < sql.length(); ++i) {
            char c = sql.charAt(i);
            char lookAhead = (index + 1 < sql.length()) ? sql.charAt(i + 1) : 0;
            if (c == '\'') {
                // skip over double single quote
                if (inSingleQuotes && lookAhead == '\'') {
                    index += 1;
                    continue;
                }
                if (inSingleQuotes) {
                    inSingleQuotes = false;
                    last = i;
                    index = i + 1;
                    break;
                }
                inSingleQuotes = true;
                start = i + 1;
            }
        }

        if (start != -1 && last != -1) {
            collector.accept(sql.substring(start, last));
        }

        return index;
    }

    private int parseWhere(String sql, int index) {
        for (int i = index; i < sql.length(); ++i) {
            // parse column name
            StringBuilder columnName = new StringBuilder();
            index = parseQuotedValue(sql, index, columnName::append);
            index += 3; // space, equals, space
            final LogMinerColumnValueImpl column = new LogMinerColumnValueImpl(columnName.toString());
            index = parseColumnValue(sql, index, column::setColumnData);
            index += 1; // space
            columns.add(column);

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

    private void reset() {
        columnName = null;
        schemaName = null;
        tableName = null;
        binary = false;
        columns = new ArrayList<>();
    }
}
