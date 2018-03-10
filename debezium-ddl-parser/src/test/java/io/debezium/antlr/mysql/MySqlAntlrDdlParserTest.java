/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr.mysql;

import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.SimpleDdlParserListener;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParserTest {

    private DdlParser parser;
    private Tables tables;
    private SimpleDdlParserListener listener;

    @Before
    public void beforeEach() {
        parser = new MySqlAntlrDdlParser();
        tables = new Tables();
        listener = new SimpleDdlParserListener();
        parser.addListener(listener);
    }

    @Test
    public void shouldParseAlterStatementsWithoutCreate() {
        String ddl = "ALTER TABLE foo ADD COLUMN c bigint;" + System.lineSeparator()
                + "ALTER TABLE foo2 ADD b bigint;";
        parser.parse(ddl, tables);
        listener.assertNext().alterTableNamed("foo").ddlStartsWith("ALTER TABLE foo ADD COLUMN c");
        listener.assertNext().alterTableNamed("foo2").ddlStartsWith("ALTER TABLE foo2 ADD b");
    }

}
