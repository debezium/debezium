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
public class DropViewParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parserCtx;

    public DropViewParserListener(MySqlAntlrDdlParser parserCtx) {
        this.parserCtx = parserCtx;
    }

    @Override
    public void enterDropView(MySqlParser.DropViewContext ctx) {
        ctx.fullId().stream().map(parserCtx::parseQualifiedTableId).forEach(tableId -> {
            parserCtx.databaseTables().removeTable(tableId);
            parserCtx.signalDropView(tableId, ctx);
        });
        super.enterDropView(ctx);
    }
}
