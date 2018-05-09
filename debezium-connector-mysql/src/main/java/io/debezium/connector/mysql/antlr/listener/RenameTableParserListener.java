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
 * Parser listeners that is parsing MySQL RENAME TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class RenameTableParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parserCtx;

    public RenameTableParserListener(MySqlAntlrDdlParser parserCtx) {
        this.parserCtx = parserCtx;
    }

    @Override
    public void enterRenameTableClause(MySqlParser.RenameTableClauseContext ctx) {
        TableId oldTable = parserCtx.parseQualifiedTableId(ctx.tableName(0).fullId());
        TableId newTable = parserCtx.parseQualifiedTableId(ctx.tableName(1).fullId());
        parserCtx.databaseTables().renameTable(oldTable, newTable);
        parserCtx.signalAlterTable(newTable, oldTable, ctx);
        super.enterRenameTableClause(ctx);
    }
}
