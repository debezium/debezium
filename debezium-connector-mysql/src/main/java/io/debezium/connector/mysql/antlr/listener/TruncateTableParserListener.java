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
    public void enterTruncateTable(MySqlParser.TruncateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        // Be aware the legacy parser is not signaling truncate events
        parser.signalTruncateTable(tableId, ctx);
        super.enterTruncateTable(ctx);
    }
}
