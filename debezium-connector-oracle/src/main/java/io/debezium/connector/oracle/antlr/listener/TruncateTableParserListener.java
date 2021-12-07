/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.TableId;

/**
 * This class is parsing Oracle truncate table statements.
 */
public class TruncateTableParserListener extends BaseParserListener {

    private final String catalogName;
    private final String schemaName;
    private final OracleDdlParser parser;

    TruncateTableParserListener(final String catalogName, final String schemaName, final OracleDdlParser parser) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    @Override
    public void enterTruncate_table(final PlSqlParser.Truncate_tableContext ctx) {
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.tableview_name()));
        parser.signalTruncateTable(tableId, ctx);
        super.enterTruncate_table(ctx);
    }
}
