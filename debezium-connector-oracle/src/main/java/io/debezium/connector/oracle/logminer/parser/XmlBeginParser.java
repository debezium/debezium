/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.relational.Table;

/**
 * A parser that interprets an Oracle LogMiner {@code XML BEGIN DOC} event type.
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public class XmlBeginParser {

    private final XmlBeginBinaryParser binaryParser = new XmlBeginBinaryParser();
    private final XmlBeginTextParser textParser = new XmlBeginTextParser();

    /**
     * An immutable object representing the parsed data of a {@code XML DOC BEGIN} operation.
     *
     * @param columnName the column name, never {@code null}
     * @param parsedEvent the parsed event data
     */
    public record XmlBegin(String columnName, LogMinerDmlEntry parsedEvent) {
    }

    /**
     * Parses a LogMiner {@code XML DOC BEGIN} event.
     *
     * @param event the event, should not be {@code null}
     * @param table the relational table, should not be {@code null}
     * @return the parsed begin event data, never {@code null}
     */
    public XmlBegin parse(LogMinerEventRow event, Table table) {
        final AbstractSingleColumnSqlRedoPreambleParser parser = getParserForEvent(event);

        final LogMinerDmlEntry result = parser.parse(event.getRedoSql(), table);
        final String columnName = parser.getColumnName();

        return new XmlBegin(columnName, result);
    }

    private AbstractSingleColumnSqlRedoPreambleParser getParserForEvent(LogMinerEventRow event) {
        if (XmlParserUtils.isXmlSerializedAsBinary(event)) {
            return binaryParser;
        }
        return textParser;
    }

    /**
     * A parser for {@code XML DOC BEGIN} operations that use binary-based storage.
     */
    private static class XmlBeginBinaryParser extends AbstractSelectSingleColumnSqlRedoPreambleParser {
        private static final String PREAMBLE = "XML DOC BEGIN:";

        XmlBeginBinaryParser() {
            super(PREAMBLE);
        }

        @Override
        protected LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues) {
            return LogMinerDmlEntryImpl.forXml(columnValues);
        }
    }

    /**
     * A parser for {@code XML DOC BEGIN} operations that use text-based storage.
     */
    private static class XmlBeginTextParser extends AbstractSingleColumnSqlRedoPreambleParser {
        private static final String UPDATE = "update ";
        private static final String ALIAS_CLAUSE = " a set a.";
        private static final String WHERE_CLAUSE = " where ";

        @Override
        protected void parseInternal(String sql, Table table) {
            int index = sql.indexOf(UPDATE);
            if (index == -1) {
                throw new IllegalStateException("Failed to locate preamble: " + UPDATE);
            }

            index = parseQuotedValue(sql, index, value -> schemaName = value);
            index += 1; // skip dot
            index = parseQuotedValue(sql, index, value -> tableName = value);

            index = indexOfThrow(ALIAS_CLAUSE, sql, index) + ALIAS_CLAUSE.length();
            index = parseQuotedValue(sql, index, value -> columnName = value);

            index = indexOfThrow(WHERE_CLAUSE, sql, index) + WHERE_CLAUSE.length();
            parseWhereClause(sql, index, table);

            ParserUtils.setColumnUnavailableValues(columnValues, table);
        }

        @Override
        protected LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues) {
            return LogMinerDmlEntryImpl.forXml(columnValues);
        }
    }
}
