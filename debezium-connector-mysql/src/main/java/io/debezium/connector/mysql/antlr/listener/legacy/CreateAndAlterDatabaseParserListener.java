/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener.legacy;

import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.mysql.antlr.MySqlPtAntlrDdlParser;
import io.debezium.ddl.parser.mysql.legacy.MySqlParser;
import io.debezium.ddl.parser.mysql.legacy.MySqlParserBaseListener;

/**
 * Parser listener that is parsing MySQL CREATE DATABASE and ALTER DATABASE statements,
 * to get default character sets for database.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class CreateAndAlterDatabaseParserListener extends MySqlParserBaseListener {

    private final MySqlPtAntlrDdlParser parser;
    private String databaseName;
    private boolean charsetSpecified;

    public CreateAndAlterDatabaseParserListener(MySqlPtAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterCreateDatabase(MySqlParser.CreateDatabaseContext ctx) {
        databaseName = parser.parseName(ctx.uid());
        charsetSpecified = false;
        super.enterCreateDatabase(ctx);
    }

    @Override
    public void exitCreateDatabase(MySqlParser.CreateDatabaseContext ctx) {
        parser.signalCreateDatabase(databaseName, ctx);
        super.exitCreateDatabase(ctx);
    }

    @Override
    public void enterAlterSimpleDatabase(MySqlParser.AlterSimpleDatabaseContext ctx) {
        databaseName = ctx.uid() == null ? parser.currentSchema() : parser.parseName(ctx.uid());
        charsetSpecified = false;
        super.enterAlterSimpleDatabase(ctx);
    }

    @Override
    public void enterCreateDatabaseOption(MySqlParser.CreateDatabaseOptionContext ctx) {
        String charsetName = parser.extractCharset(ctx.charsetName(), ctx.collationName());
        if (ctx.charsetName() != null) {
            if ("DEFAULT".equalsIgnoreCase(charsetName)) {
                charsetName = parser.systemVariables().getVariable(BinlogSystemVariables.CHARSET_NAME_SERVER);
            }
            parser.charsetNameForDatabase().put(databaseName, charsetName);
            charsetSpecified = true;
        }
        // Collation is used only if the statement does not set the charset explicitly
        else if (ctx.collationName() != null && charsetName != null && !charsetSpecified) {
            parser.charsetNameForDatabase().put(databaseName, charsetName);
        }
        super.enterCreateDatabaseOption(ctx);
    }
}
