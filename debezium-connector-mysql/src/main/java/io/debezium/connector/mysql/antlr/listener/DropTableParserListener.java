/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import org.antlr.v4.runtime.misc.Interval;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableId;

/**
 * Parser listener that is parsing MySQL DROP TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class DropTableParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;

    public DropTableParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterDropTable(MySqlParser.DropTableContext ctx) {
        // Get the parent dropStatement context to include "DROP" keyword
        int startIndex = ctx.getParent().getStart().getStartIndex();
        Interval interval = new Interval(startIndex, ctx.tableRefList().start.getStartIndex() - 1);
        String prefix = ctx.start.getInputStream().getText(interval);

        // Determine if RESTRICT or CASCADE is specified
        String dropType = null;
        if (ctx.RESTRICT_SYMBOL() != null) {
            dropType = "RESTRICT";
        }
        else if (ctx.CASCADE_SYMBOL() != null) {
            dropType = "CASCADE";
        }

        final String finalDropType = dropType;
        ctx.tableRefList().tableRef().forEach(tableRefContext -> {
            TableId tableId = parser.parseQualifiedTableId(tableRefContext);
            parser.databaseTables().removeTable(tableId);
            parser.signalDropTable(tableId, prefix + tableId.toQuotedString('`')
                    + (finalDropType != null ? " " + finalDropType : ""));
        });
        super.enterDropTable(ctx);
    }
}
