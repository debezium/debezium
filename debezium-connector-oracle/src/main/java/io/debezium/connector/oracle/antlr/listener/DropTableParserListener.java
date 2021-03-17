/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import static io.debezium.connector.oracle.antlr.listener.ParserUtils.getTableName;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;
import io.debezium.relational.TableId;

/**
 * This class is parsing Oracle drop table statements.
 */
public class DropTableParserListener extends PlSqlParserBaseListener {

    private String catalogName;
    private String schemaName;
    private OracleDdlParser parser;

    DropTableParserListener(final String catalogName, final String schemaName, final OracleDdlParser parser) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    @Override
    public void enterDrop_table(final PlSqlParser.Drop_tableContext ctx) {
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.tableview_name()));
        parser.databaseTables().removeTable(tableId);
        super.enterDrop_table(ctx);
    }
}
