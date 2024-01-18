/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;

/**
 * Parser listener that is parsing MySQL SET statements, for defining a system variables.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class SetStatementParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;

    public SetStatementParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterSetVariable(MySqlParser.SetVariableContext ctx) {
        // If you set multiple system variables, the most recent GLOBAL or SESSION modifier in the statement
        // is used for following assignments that have no modifier specified.
        MySqlSystemVariables.MySqlScope scope = null;
        for (int i = 0; i < ctx.variableClause().size(); i++) {
            MySqlParser.VariableClauseContext variableClauseContext = ctx.variableClause(i);
            String variableName;
            if (variableClauseContext.uid() == null) {
                if (variableClauseContext.GLOBAL_ID() == null) {
                    // that mean that user variable is set, so do nothing with it
                    continue;
                }
                String variableIdentifier = variableClauseContext.GLOBAL_ID().getText();
                if (variableIdentifier.startsWith("@@global.")) {
                    scope = MySqlSystemVariables.MySqlScope.GLOBAL;
                    variableName = variableIdentifier.substring("@@global.".length());
                }
                else if (variableIdentifier.startsWith("@@session.")) {
                    scope = MySqlSystemVariables.MySqlScope.SESSION;
                    variableName = variableIdentifier.substring("@@session.".length());
                }
                else if (variableIdentifier.startsWith("@@local.")) {
                    scope = MySqlSystemVariables.MySqlScope.LOCAL;
                    variableName = variableIdentifier.substring("@@local.".length());
                }
                else {
                    scope = MySqlSystemVariables.MySqlScope.SESSION;
                    variableName = variableIdentifier.substring("@@".length());
                }
            }
            else {
                if (variableClauseContext.GLOBAL() != null) {
                    scope = MySqlSystemVariables.MySqlScope.GLOBAL;
                }
                else if (variableClauseContext.SESSION() != null) {
                    scope = MySqlSystemVariables.MySqlScope.SESSION;
                }
                else if (variableClauseContext.LOCAL() != null) {
                    scope = MySqlSystemVariables.MySqlScope.LOCAL;
                }

                variableName = parser.parseName(variableClauseContext.uid());
            }
            String value = parser.withoutQuotes(ctx.expression(i));

            parser.systemVariables().setVariable(scope, variableName, value);

            // If this is setting 'character_set_database', then we need to record the character set for
            // the given database ...
            if (MySqlSystemVariables.CHARSET_NAME_DATABASE.equalsIgnoreCase(variableName)) {
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
    public void enterSetCharset(MySqlParser.SetCharsetContext ctx) {
        String charsetName = ctx.charsetName() != null ? parser.withoutQuotes(ctx.charsetName()) : parser.currentDatabaseCharset();
        // Sets variables according to documentation at
        // https://dev.mysql.com/doc/refman/8.2/en/set-character-set.html
        // Using default scope for these variables, because this type of set statement you cannot specify
        // the scope manually
        parser.systemVariables().setVariable(MySqlSystemVariables.MySqlScope.SESSION, MySqlSystemVariables.CHARSET_NAME_CLIENT, charsetName);
        parser.systemVariables().setVariable(MySqlSystemVariables.MySqlScope.SESSION, MySqlSystemVariables.CHARSET_NAME_RESULT, charsetName);
        parser.systemVariables().setVariable(MySqlSystemVariables.MySqlScope.SESSION, MySqlSystemVariables.CHARSET_NAME_CONNECTION,
                parser.systemVariables().getVariable(MySqlSystemVariables.CHARSET_NAME_DATABASE));
        super.enterSetCharset(ctx);
    }

    @Override
    public void enterSetNames(MySqlParser.SetNamesContext ctx) {
        String charsetName = ctx.charsetName() != null ? parser.withoutQuotes(ctx.charsetName()) : parser.currentDatabaseCharset();
        // Sets variables according to documentation at
        // https://dev.mysql.com/doc/refman/8.2/en/set-names.html
        // Using default scope for these variables, because this type of set statement you cannot specify
        // the scope manually
        parser.systemVariables().setVariable(MySqlSystemVariables.MySqlScope.SESSION, MySqlSystemVariables.CHARSET_NAME_CLIENT, charsetName);
        parser.systemVariables().setVariable(MySqlSystemVariables.MySqlScope.SESSION, MySqlSystemVariables.CHARSET_NAME_RESULT, charsetName);
        parser.systemVariables().setVariable(MySqlSystemVariables.MySqlScope.SESSION, MySqlSystemVariables.CHARSET_NAME_CONNECTION, charsetName);
        super.enterSetNames(ctx);
    }
}
