/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.annotation.NotThreadSafe;

/**
 * Simple text-based parser implementation for Oracle LogMiner XML_BEGIN Redo SQL.
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public class XmlBeginParser extends AbstractSelectSingleColumnSqlRedoPreambleParser {

    private static final String PREAMBLE = "XML DOC BEGIN:";

    public XmlBeginParser() {
        super(PREAMBLE);
    }

    @Override
    protected LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues) {
        return LogMinerDmlEntryImpl.forXml(columnValues);
    }

}
