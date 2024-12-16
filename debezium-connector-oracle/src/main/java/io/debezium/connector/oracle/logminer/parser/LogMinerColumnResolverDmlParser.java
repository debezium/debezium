/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.relational.Table;

/**
 * A specialized implementation of {@link LogMinerDmlParser} that aims to map the column names when a
 * SQL reconstruction by LogMiner fails from {@code COLx} format to the actual column name using the
 * relational table model.
 * <p>
 * This implementation is only used when detecting a SQL reconstruction failure to minimize the total
 * parser overhead to be applied only in this circumstance.
 *
 * @author Chris Cranford
 */
public class LogMinerColumnResolverDmlParser extends LogMinerDmlParser {

    public LogMinerColumnResolverDmlParser(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    protected int getColumnIndexByName(String columnName, Table table) {
        if (columnName.matches("COL \\d*")) {
            // LogMiner always uses 1-based indices, 0-based indices are required.
            return Integer.parseInt(columnName.substring(4)) - 1;
        }
        return super.getColumnIndexByName(columnName, table);
    }
}
