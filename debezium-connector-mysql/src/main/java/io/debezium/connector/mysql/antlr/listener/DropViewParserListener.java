/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;

/**
 * Parser listener that is parsing MySQL DROP VIEW statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class DropViewParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;

    public DropViewParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterDropView(MySqlParser.DropViewContext ctx) {
        if (!parser.skipViews()) {
            ctx.fullId().stream().map(parser::parseQualifiedTableId).forEach(tableId -> {
                parser.databaseTables().removeTable(tableId);
                parser.signalDropView(tableId, ctx);
            });
        }
        super.enterDropView(ctx);
    }
}
