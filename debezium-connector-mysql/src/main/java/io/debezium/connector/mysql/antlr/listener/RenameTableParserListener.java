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
 * Parser listener that is parsing MySQL RENAME TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class RenameTableParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;

    public RenameTableParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterRenameTableClause(MySqlParser.RenameTableClauseContext ctx) {
        TableId oldTable = parser.parseQualifiedTableId(ctx.tableName(0).fullId());
        TableId newTable = parser.parseQualifiedTableId(ctx.tableName(1).fullId());
        parser.databaseTables().renameTable(oldTable, newTable);
        parser.signalAlterTable(newTable, oldTable, ctx);
        super.enterRenameTableClause(ctx);
    }
}
