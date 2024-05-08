/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import org.antlr.v4.runtime.misc.Interval;

import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParserBaseListener;
import io.debezium.relational.TableId;

/**
 * Parser listener for parsing DROP TABLE statements.
 *
 * @author Chris Cranford
 */
public class DropTableParserListener extends MariaDBParserBaseListener {

    private final MariaDbAntlrDdlParser parser;

    public DropTableParserListener(MariaDbAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterDropTable(MariaDBParser.DropTableContext ctx) {
        Interval interval = new Interval(ctx.start.getStartIndex(), ctx.tables().start.getStartIndex() - 1);
        String prefix = ctx.start.getInputStream().getText(interval);
        ctx.tables().tableName().forEach(tableNameContext -> {
            TableId tableId = parser.parseQualifiedTableId(tableNameContext.fullId());
            parser.databaseTables().removeTable(tableId);
            parser.signalDropTable(tableId, prefix + tableId.toQuotedString('`')
                    + (ctx.dropType != null ? " " + ctx.dropType.getText() : ""));
        });
        super.enterDropTable(ctx);
    }
}
