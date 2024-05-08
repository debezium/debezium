/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.relational.Table;

/**
 * Contract for a DML parser for LogMiner.
 *
 * @author Chris Cranford
 */
public interface DmlParser {
    /**
     * Parse a DML SQL string from the LogMiner event stream.
     *
     * @param sql the sql statement
     * @param table the table the sql statement is for
     * @return the parsed sql as a DML entry or {@code null} if the SQL couldn't be parsed.
     * @throws DmlParserException thrown if a parse exception is detected.
     */
    LogMinerDmlEntry parse(String sql, Table table);
}
