/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.history;

import io.debezium.relational.SystemVariables;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.AbstractDdlParser;

/**
 * @author Ismail simsek
 */
public class TestAntlrDdlParser extends AbstractDdlParser {
    public TestAntlrDdlParser() {
        super(false, false);
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        return null;
    }

    @Override
    public void parse(String ddlContent, Tables databaseTables) {

    }
}
