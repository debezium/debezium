/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableId;
import org.antlr.v4.runtime.misc.Interval;

/**
 * Parser listeners that is parsing MySQL DROP TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class DropTableParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parserCtx;

    public DropTableParserListener(MySqlAntlrDdlParser parserCtx) {
        this.parserCtx = parserCtx;
    }

    @Override
    public void enterDropTable(MySqlParser.DropTableContext ctx) {
        Interval interval = new Interval(ctx.start.getStartIndex(), ctx.tables().start.getStartIndex() - 1);
        String prefix = ctx.start.getInputStream().getText(interval);
        ctx.tables().tableName().forEach(tableNameContext -> {
            TableId tableId = parserCtx.parseQualifiedTableId(tableNameContext.fullId());
            parserCtx.databaseTables().removeTable(tableId);
            parserCtx.signalDropTable(tableId, prefix + tableId.table()
                    + (ctx.dropType != null ? " " + ctx.dropType.getText() : ""));
        });
        super.enterDropTable(ctx);
    }
}
