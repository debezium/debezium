/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;

/**
 * Parser listener that is parsing MySQL CREATE DATABASE and ALTER DATABASE statements,
 * to get default character sets for database.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class CreateAndAlterDatabaseParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;
    private String databaseName;

    public CreateAndAlterDatabaseParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterCreateDatabase(MySqlParser.CreateDatabaseContext ctx) {
        databaseName = parser.parseName(ctx.schemaName().identifier());
        super.enterCreateDatabase(ctx);
    }

    @Override
    public void exitCreateDatabase(MySqlParser.CreateDatabaseContext ctx) {
        // Use parent context to include CREATE keyword
        parser.signalCreateDatabase(databaseName, ctx.getParent());
        databaseName = null; // Clear to prevent table charset from overwriting database charset
        super.exitCreateDatabase(ctx);
    }

    @Override
    public void enterAlterDatabase(MySqlParser.AlterDatabaseContext ctx) {
        databaseName = parser.parseName(ctx.schemaRef().identifier());
        super.enterAlterDatabase(ctx);
    }

    @Override
    public void exitAlterDatabase(MySqlParser.AlterDatabaseContext ctx) {
        databaseName = null; // Clear to prevent table charset from overwriting database charset
        super.exitAlterDatabase(ctx);
    }

    @Override
    public void enterDefaultCharset(MySqlParser.DefaultCharsetContext ctx) {
        // Only process if we're in a CREATE/ALTER DATABASE context (databaseName is set)
        // This method can also be triggered by CREATE TABLE with DEFAULT CHARSET
        if (databaseName != null) {
            String charsetName = parser.withoutQuotes(ctx.charsetName());
            if ("DEFAULT".equalsIgnoreCase(charsetName)) {
                charsetName = parser.systemVariables().getVariable(BinlogSystemVariables.CHARSET_NAME_SERVER);
            }
            parser.charsetNameForDatabase().put(databaseName, charsetName);
        }
        super.enterDefaultCharset(ctx);
    }

    @Override
    public void enterDefaultCollation(MySqlParser.DefaultCollationContext ctx) {
        // Only process if we're in a CREATE/ALTER DATABASE context (databaseName is set)
        // This method can also be triggered by CREATE TABLE with DEFAULT COLLATE
        if (databaseName != null) {
            // Collation is used only if the database charset was not set by charset setting
            if (!parser.charsetNameForDatabase().containsKey(databaseName)) {
                String charsetName = parser.extractCharset(null, ctx.collationName());
                if (charsetName != null) {
                    parser.charsetNameForDatabase().put(databaseName, charsetName);
                }
            }
        }
        super.enterDefaultCollation(ctx);
    }
}
