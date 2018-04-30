/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr.mysql.listener;

import io.debezium.antlr.mysql.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableId;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class TruncateTableParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parserCtx;

    public TruncateTableParserListener(MySqlAntlrDdlParser parserCtx) {
        this.parserCtx = parserCtx;
    }

    @Override
    public void enterTruncateTable(MySqlParser.TruncateTableContext ctx) {
        TableId tableId = parserCtx.parseQualifiedTableId(ctx.tableName().fullId());
        // Be aware the legacy parser is not signaling truncate events
        parserCtx.signalTruncateTable(tableId, ctx);
        super.enterTruncateTable(ctx);
    }
}
