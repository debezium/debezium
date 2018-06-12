/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr.mysql.listener;

import io.debezium.antlr.mysql.MySqlAntlrDdlParser;
import io.debezium.antlr.mysql.MySqlSystemVariables;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;

/**
 * Parser listener for MySQL create database query to get database charsetName.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class DatabaseOptionsParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parserCtx;
    private String databaseName;

    public DatabaseOptionsParserListener(MySqlAntlrDdlParser parserCtx) {
        this.parserCtx = parserCtx;
    }

    @Override
    public void enterCreateDatabase(MySqlParser.CreateDatabaseContext ctx) {
        databaseName = parserCtx.parseName(ctx.uid());
        super.enterCreateDatabase(ctx);
    }

    @Override
    public void exitCreateDatabase(MySqlParser.CreateDatabaseContext ctx) {
        parserCtx.signalCreateDatabase(databaseName, ctx);
        super.exitCreateDatabase(ctx);
    }

    @Override
    public void enterAlterSimpleDatabase(MySqlParser.AlterSimpleDatabaseContext ctx) {
        databaseName = ctx.uid() == null ? parserCtx.currentSchema() : parserCtx.parseName(ctx.uid());
        super.enterAlterSimpleDatabase(ctx);
    }

    @Override
    public void enterCreateDatabaseOption(MySqlParser.CreateDatabaseOptionContext ctx) {
        if (ctx.charsetName() != null) {
            String charsetName = parserCtx.withoutQuotes(ctx.charsetName());
            if ("DEFAULT".equalsIgnoreCase(charsetName)) {
                charsetName = parserCtx.systemVariables().getVariable(MySqlSystemVariables.CHARSET_NAME_SERVER);
            }
            parserCtx.charsetNameForDatabase().put(databaseName, charsetName);
        }
        super.enterCreateDatabaseOption(ctx);
    }
}
