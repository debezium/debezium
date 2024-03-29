/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables.BinlogScope;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParserBaseListener;

/**
 * Parser listener for parsing SET statements, which define system variables.
 *
 * @author Chris Cranford
 */
public class SetStatementParserListener extends MariaDBParserBaseListener {

    private final MariaDbAntlrDdlParser parser;

    public SetStatementParserListener(MariaDbAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterSetVariable(MariaDBParser.SetVariableContext ctx) {
        // If you set multiple system variables, the most recent GLOBAL or SESSION modifier in the statement
        // is used for following assignments that have no modifier specified.
        BinlogScope scope = null;
        for (int i = 0; i < ctx.variableClause().size(); i++) {
            MariaDBParser.VariableClauseContext variableClauseContext = ctx.variableClause(i);
            String variableName;
            if (variableClauseContext.uid() == null) {
                if (variableClauseContext.GLOBAL_ID() == null) {
                    // that mean that user variable is set, so do nothing with it
                    continue;
                }
                String variableIdentifier = variableClauseContext.GLOBAL_ID().getText();
                if (variableIdentifier.startsWith("@@global.")) {
                    scope = BinlogScope.GLOBAL;
                    variableName = variableIdentifier.substring("@@global.".length());
                }
                else if (variableIdentifier.startsWith("@@session.")) {
                    scope = BinlogScope.SESSION;
                    variableName = variableIdentifier.substring("@@session.".length());
                }
                else if (variableIdentifier.startsWith("@@local.")) {
                    scope = BinlogScope.LOCAL;
                    variableName = variableIdentifier.substring("@@local.".length());
                }
                else {
                    scope = BinlogScope.SESSION;
                    variableName = variableIdentifier.substring("@@".length());
                }
            }
            else {
                if (variableClauseContext.GLOBAL() != null) {
                    scope = BinlogScope.GLOBAL;
                }
                else if (variableClauseContext.SESSION() != null) {
                    scope = BinlogScope.SESSION;
                }
                else if (variableClauseContext.LOCAL() != null) {
                    scope = BinlogScope.LOCAL;
                }

                variableName = parser.parseName(variableClauseContext.uid());
            }
            String value = parser.withoutQuotes(ctx.expression(i));

            parser.systemVariables().setVariable(scope, variableName, value);

            // If this is setting 'character_set_database', then we need to record the character set for
            // the given database ...
            if (BinlogSystemVariables.CHARSET_NAME_DATABASE.equalsIgnoreCase(variableName)) {
                String currentDatabaseName = parser.currentSchema();
                if (currentDatabaseName != null) {
                    parser.charsetNameForDatabase().put(currentDatabaseName, value);
                }
            }

            // Signal that the variable was set ...
            parser.signalSetVariable(variableName, value, i, ctx);
        }
        super.enterSetVariable(ctx);
    }

    @Override
    public void enterSetCharset(MariaDBParser.SetCharsetContext ctx) {
        String charsetName = ctx.charsetName() != null ? parser.withoutQuotes(ctx.charsetName()) : parser.currentDatabaseCharset();
        // Sets variables according to documentation at
        // https://mariadb.com/kb/en/set-character-set/
        // Using default scope for these variables, because this type of set statement you cannot specify
        // the scope manually
        parser.systemVariables().setVariable(BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_CLIENT, charsetName);
        parser.systemVariables().setVariable(BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_RESULT, charsetName);
        parser.systemVariables().setVariable(BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_CONNECTION,
                parser.systemVariables().getVariable(BinlogSystemVariables.CHARSET_NAME_DATABASE));
        super.enterSetCharset(ctx);
    }

    @Override
    public void enterSetNames(MariaDBParser.SetNamesContext ctx) {
        String charsetName = ctx.charsetName() != null ? parser.withoutQuotes(ctx.charsetName()) : parser.currentDatabaseCharset();
        // Sets variables according to documentation at
        // https://mariadb.com/kb/en/set-names/
        // Using default scope for these variables, because this type of set statement you cannot specify
        // the scope manually
        parser.systemVariables().setVariable(BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_CLIENT, charsetName);
        parser.systemVariables().setVariable(BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_RESULT, charsetName);
        parser.systemVariables().setVariable(BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_CONNECTION, charsetName);
        super.enterSetNames(ctx);
    }
}
