/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener.legacy;

import io.debezium.connector.mysql.antlr.MySqlPtAntlrDdlParser;
import io.debezium.ddl.parser.mysql.legacy.MySqlParser;
import io.debezium.ddl.parser.mysql.legacy.MySqlParserBaseListener;

/**
 * Parser listener that is parsing MySQL DROP DATABASE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class DropDatabaseParserListener extends MySqlParserBaseListener {

    private final MySqlPtAntlrDdlParser parser;

    public DropDatabaseParserListener(MySqlPtAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterDropDatabase(MySqlParser.DropDatabaseContext ctx) {
        String databaseName = parser.parseName(ctx.uid());
        parser.databaseTables().removeTablesForDatabase(databaseName);
        parser.charsetNameForDatabase().remove(databaseName);
        parser.signalDropDatabase(databaseName, ctx);
        super.enterDropDatabase(ctx);
    }
}
