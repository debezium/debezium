/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParserBaseListener;

/**
 * Parser listener for CREATE DATABASE and ALTER DATABASE statements.
 *
 * @author Chris Cranford
 */
public class CreateAndAlterDatabaseParserListener extends MariaDBParserBaseListener {

    private final MariaDbAntlrDdlParser parser;
    private String databaseName;

    public CreateAndAlterDatabaseParserListener(MariaDbAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterCreateDatabase(MariaDBParser.CreateDatabaseContext ctx) {
        databaseName = parser.parseName(ctx.uid());
        super.enterCreateDatabase(ctx);
    }

    @Override
    public void exitCreateDatabase(MariaDBParser.CreateDatabaseContext ctx) {
        parser.signalCreateDatabase(databaseName, ctx);
        super.exitCreateDatabase(ctx);
    }

    @Override
    public void enterAlterSimpleDatabase(MariaDBParser.AlterSimpleDatabaseContext ctx) {
        databaseName = ctx.uid() == null ? parser.currentSchema() : parser.parseName(ctx.uid());
        super.enterAlterSimpleDatabase(ctx);
    }

    @Override
    public void enterCreateDatabaseOption(MariaDBParser.CreateDatabaseOptionContext ctx) {
        String charsetName = parser.extractCharset(ctx.charsetName(), ctx.collationName());
        if (ctx.charsetName() != null) {
            if ("DEFAULT".equalsIgnoreCase(charsetName)) {
                charsetName = parser.systemVariables().getVariable(BinlogSystemVariables.CHARSET_NAME_SERVER);
            }
            parser.charsetNameForDatabase().put(databaseName, charsetName);
        }
        // Collation is used only if the database charset was not set by charset setting
        else if (ctx.charsetName() != null && !parser.charsetNameForDatabase().containsKey(charsetName)) {
            parser.charsetNameForDatabase().put(databaseName, charsetName);
        }
        super.enterCreateDatabaseOption(ctx);
    }
}
