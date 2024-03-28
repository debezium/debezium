/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;

/**
 * Parser listener for parsing DROP VIEW statements.
 *
 * @author Chris Cranford
 */
public class DropViewParserListener extends MySqlParserBaseListener {

    private final MariaDbAntlrDdlParser parser;

    public DropViewParserListener(MariaDbAntlrDdlParser parser) {
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
