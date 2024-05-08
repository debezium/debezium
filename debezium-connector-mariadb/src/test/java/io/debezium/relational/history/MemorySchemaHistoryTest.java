/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Chris Cranford
 */
public class MemorySchemaHistoryTest extends AbstractMemorySchemaHistoryTest<MariaDbConnector> {
    @Override
    protected DdlParser getDdlParser() {
        return new MariaDbAntlrDdlParser();
    }
}
