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
    private boolean charsetSpecified;

    public CreateAndAlterDatabaseParserListener(MariaDbAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterCreateDatabase(MariaDBParser.CreateDatabaseContext ctx) {
        databaseName = parser.parseName(ctx.uid());
        charsetSpecified = false;
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
        charsetSpecified = false;
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
            charsetSpecified = true;
        }
        // Collation is used only if the statement does not set the charset explicitly
        else if (ctx.collationName() != null && charsetName != null && !charsetSpecified) {
            parser.charsetNameForDatabase().put(databaseName, charsetName);
        }
        super.enterCreateDatabaseOption(ctx);
    }
}
