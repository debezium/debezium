/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr.mysql.listener;

import io.debezium.antlr.mysql.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class DropDatabaseParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parserCtx;

    public DropDatabaseParserListener(MySqlAntlrDdlParser parserCtx) {
        this.parserCtx = parserCtx;
    }

    @Override
    public void enterDropDatabase(MySqlParser.DropDatabaseContext ctx) {
        String databaseName = parserCtx.parseName(ctx.uid());
        parserCtx.databaseTables().removeTablesForDatabase(databaseName);
        parserCtx.charsetNameForDatabase().remove(databaseName);
        parserCtx.signalDropDatabase(databaseName, ctx);
        super.enterDropDatabase(ctx);
    }
}
