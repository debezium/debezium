/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.annotation.NotThreadSafe;

/**
 * Simple text-based parser implementation for Oracle LogMiner SEL_LOB_LOCATOR Redo SQL.
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public class SelectLobParser extends AbstractSelectSingleColumnSqlRedoPreambleParser {

    private static final String BLOB_LOCATOR = "loc_b";
    private static final String BLOB_BUFFER = "buf_b";

    private boolean binary;

    public SelectLobParser() {
        super(BEGIN);
    }

    public boolean isBinary() {
        return binary;
    }

    @Override
    protected void reset() {
        this.binary = false;
    }

    @Override
    protected int parseIntoClause(String sql, int index) {
        if (sql.indexOf(BLOB_LOCATOR, index) == index || sql.indexOf(BLOB_BUFFER, index) == index) {
            binary = true;
        }
        return sql.indexOf(" ", index) + 1;
    }

    @Override
    protected LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues) {
        return LogMinerDmlEntryImpl.forLobLocator(columnValues);
    }

}
