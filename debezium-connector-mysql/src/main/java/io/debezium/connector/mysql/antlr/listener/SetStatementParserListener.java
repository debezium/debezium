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
 * Parser listener that is parsing MySQL SET statements, for defining a system variables.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class SetStatementParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;
    private BinlogSystemVariables.BinlogScope currentScope = null;
    private int optionIndex = 0;
    private MySqlParser.SetStatementContext setStatementContext = null;

    public SetStatementParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterSetStatement(MySqlParser.SetStatementContext ctx) {
        currentScope = null;
        optionIndex = 0;
        setStatementContext = ctx;
        super.enterSetStatement(ctx);
    }

    @Override
    public void enterOptionType(MySqlParser.OptionTypeContext ctx) {
        // Capture the scope for subsequent variables
        if (ctx.GLOBAL_SYMBOL() != null) {
            currentScope = BinlogSystemVariables.BinlogScope.GLOBAL;
        }
        else if (ctx.SESSION_SYMBOL() != null || ctx.LOCAL_SYMBOL() != null) {
            currentScope = BinlogSystemVariables.BinlogScope.SESSION;
        }
        super.enterOptionType(ctx);
    }

    @Override
    public void enterOptionValueNoOptionType(MySqlParser.OptionValueNoOptionTypeContext ctx) {
        // Handle SET NAMES
        if (ctx.NAMES_SYMBOL() != null) {
            String charsetName;
            if (ctx.charsetName() != null) {
                charsetName = parser.withoutQuotes(ctx.charsetName().getText());
                if ("default".equalsIgnoreCase(charsetName)) {
                    charsetName = parser.currentDatabaseCharset();
                }
            }
            else if (ctx.DEFAULT_SYMBOL() != null) {
                charsetName = parser.currentDatabaseCharset();
            }
            else {
                charsetName = parser.currentDatabaseCharset();
            }

            // Sets variables according to documentation at
            // https://dev.mysql.com/doc/refman/8.2/en/set-names.html
            parser.systemVariables().setVariable(BinlogSystemVariables.BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_CLIENT, charsetName);
            parser.systemVariables().setVariable(BinlogSystemVariables.BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_RESULT, charsetName);
            parser.systemVariables().setVariable(BinlogSystemVariables.BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_CONNECTION, charsetName);
        }
        // Handle SET CHARSET / SET CHARACTER SET
        else if (ctx.charset() != null) {
            String charsetName;
            if (ctx.charsetName() != null) {
                charsetName = parser.withoutQuotes(ctx.charsetName().getText());
                if ("default".equalsIgnoreCase(charsetName)) {
                    charsetName = parser.currentDatabaseCharset();
                }
            }
            else if (ctx.DEFAULT_SYMBOL() != null) {
                charsetName = parser.currentDatabaseCharset();
            }
            else {
                charsetName = parser.currentDatabaseCharset();
            }

            // Sets variables according to documentation at
            // https://dev.mysql.com/doc/refman/8.2/en/set-character-set.html
            parser.systemVariables().setVariable(BinlogSystemVariables.BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_CLIENT, charsetName);
            parser.systemVariables().setVariable(BinlogSystemVariables.BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_RESULT, charsetName);
            parser.systemVariables().setVariable(BinlogSystemVariables.BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_CONNECTION,
                    parser.systemVariables().getVariable(BinlogSystemVariables.CHARSET_NAME_DATABASE));
        }
        // Handle @@variable assignments - MUST come before regular variable assignments
        else if (ctx.AT_AT_SIGN_SYMBOL() != null && ctx.lvalueVariable() != null) {
            BinlogSystemVariables.BinlogScope scope = parseSystemVariableScope(ctx);
            handleVariableAssignment(ctx.lvalueVariable(), ctx.setExprOrDefault(), scope, ctx);
        }
        // Handle regular variable assignments (lvalueVariable equal setExprOrDefault)
        else if (ctx.lvalueVariable() != null && ctx.equal() != null && ctx.setExprOrDefault() != null) {
            handleVariableAssignment(ctx.lvalueVariable(), ctx.setExprOrDefault(), currentScope, ctx);
        }

        super.enterOptionValueNoOptionType(ctx);
    }

    @Override
    public void enterOptionValue(MySqlParser.OptionValueContext ctx) {
        // Handle optionType lvalueVariable equal setExprOrDefault
        if (ctx.optionType() != null && ctx.lvalueVariable() != null) {
            BinlogSystemVariables.BinlogScope scope = null;
            if (ctx.optionType().GLOBAL_SYMBOL() != null) {
                scope = BinlogSystemVariables.BinlogScope.GLOBAL;
            }
            else if (ctx.optionType().SESSION_SYMBOL() != null || ctx.optionType().LOCAL_SYMBOL() != null) {
                scope = BinlogSystemVariables.BinlogScope.SESSION;
            }
            handleVariableAssignment(ctx.lvalueVariable(), ctx.setExprOrDefault(), scope, ctx);
        }
        super.enterOptionValue(ctx);
    }

    @Override
    public void enterOptionValueFollowingOptionType(MySqlParser.OptionValueFollowingOptionTypeContext ctx) {
        // This is called for SET GLOBAL/SESSION/LOCAL var=value
        // The scope was already captured in enterOptionType
        if (ctx.lvalueVariable() != null) {
            handleVariableAssignment(ctx.lvalueVariable(), ctx.setExprOrDefault(), currentScope, ctx);
        }
        super.enterOptionValueFollowingOptionType(ctx);
    }

    private BinlogSystemVariables.BinlogScope parseSystemVariableScope(MySqlParser.OptionValueNoOptionTypeContext ctx) {
        if (ctx.setVarIdentType() != null) {
            if (ctx.setVarIdentType().GLOBAL_SYMBOL() != null) {
                return BinlogSystemVariables.BinlogScope.GLOBAL;
            }
            else if (ctx.setVarIdentType().SESSION_SYMBOL() != null || ctx.setVarIdentType().LOCAL_SYMBOL() != null) {
                return BinlogSystemVariables.BinlogScope.SESSION;
            }
        }
        return BinlogSystemVariables.BinlogScope.SESSION;
    }

    private void handleVariableAssignment(MySqlParser.LvalueVariableContext varCtx,
                                          MySqlParser.SetExprOrDefaultContext valueCtx,
                                          BinlogSystemVariables.BinlogScope scope,
                                          org.antlr.v4.runtime.ParserRuleContext ctx) {
        String variableName = parseVariableName(varCtx);
        if (variableName == null) {
            return; // User variable, ignore
        }

        String value = valueCtx != null ? parser.withoutQuotes(valueCtx.getText()) : null;

        BinlogSystemVariables.BinlogScope effectiveScope = scope != null ? scope : currentScope;
        if (effectiveScope == null) {
            effectiveScope = BinlogSystemVariables.BinlogScope.SESSION;
        }

        parser.systemVariables().setVariable(effectiveScope, variableName, value);

        // If this is setting 'character_set_database', then we need to record the character set for
        // the given database ...
        if (BinlogSystemVariables.CHARSET_NAME_DATABASE.equalsIgnoreCase(variableName)) {
            String currentDatabaseName = parser.currentSchema();
            if (currentDatabaseName != null) {
                parser.charsetNameForDatabase().put(currentDatabaseName, value);
            }
        }

        // Signal that the variable was set - use parent setStatement context to include SET keyword
        parser.signalSetVariable(variableName, value, optionIndex++, setStatementContext != null ? setStatementContext : ctx);
    }

    private String parseVariableName(MySqlParser.LvalueVariableContext varCtx) {
        if (varCtx.identifier() != null) {
            return parser.parseName(varCtx.identifier());
        }
        // If it's a user variable (starts with @), ignore it
        return null;
    }
}
