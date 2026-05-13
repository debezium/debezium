/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import org.antlr.v4.runtime.ParserRuleContext;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableId;

/**
 * Parser listener that is parsing MySQL TRUNCATE TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class TruncateTableParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;

    public TruncateTableParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterTruncateTableStatement(MySqlParser.TruncateTableStatementContext ctx) {
        // Skip TRUNCATE statements inside EVENT/TRIGGER/PROCEDURE/FUNCTION bodies
        if (isInsideRoutineBody(ctx)) {
            return;
        }
        TableId tableId = parser.parseQualifiedTableId(ctx.tableRef());
        // Be aware the legacy parser is not signaling truncate events
        parser.signalTruncateTable(tableId, ctx);
        super.enterTruncateTableStatement(ctx);
    }

    private boolean isInsideRoutineBody(ParserRuleContext ctx) {
        ParserRuleContext parent = ctx.getParent();
        while (parent != null) {
            if (parent instanceof MySqlParser.CreateEventContext ||
                    parent instanceof MySqlParser.AlterEventContext ||
                    parent instanceof MySqlParser.CreateTriggerContext ||
                    parent instanceof MySqlParser.CreateProcedureContext ||
                    parent instanceof MySqlParser.CreateFunctionContext) {
                return true;
            }
            parent = parent.getParent();
        }
        return false;
    }
}
