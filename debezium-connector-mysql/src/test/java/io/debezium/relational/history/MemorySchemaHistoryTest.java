/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Randall Hauch
 */
public class MemorySchemaHistoryTest extends AbstractMemorySchemaHistoryTest<MySqlConnector> {
    @Override
    protected DdlParser getDdlParser() {
        return new MySqlAntlrDdlParser();
    }
}
