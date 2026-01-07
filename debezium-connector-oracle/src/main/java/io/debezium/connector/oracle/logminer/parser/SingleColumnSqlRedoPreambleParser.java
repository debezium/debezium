/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.relational.Table;
import io.debezium.text.ParsingException;

/**
 * Basic contract for parsers that can decode a single column's preamble sequence.
 *
 * @author Chris Cranford
 */
public interface SingleColumnSqlRedoPreambleParser {
    /**
     * Parses the redo SQL. Must be called before fetching state from the parser.
     *
     * @param sql the redo SQL to be parsed, can be {@code null}
     * @param table the table the redo SQL is related to, should not be {@code null}
     * @return the preamble parse data
     * @throws ParsingException if the parser failed to parse the redo SQL
     */
    LogMinerDmlEntry parse(String sql, Table table) throws ParsingException;

    /**
     * Get the schema name of the parsed statement.
     *
     * @return the schema name
     */
    String getSchemaName();

    /**
     * Get the table name of the parsed statement.
     *
     * @return the table name
     */
    String getTableName();

    /**
     * Get the name of the parsed column
     *
     * @return the column name
     */
    String getColumnName();
}
