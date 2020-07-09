/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueWrapper;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntryImpl;
import io.debezium.data.Envelope;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;

/**
 * This class parses delete statements.
 * LogMiner instruments all the values in WHERE cause regardless of original statement.
 * In other words if the original statement is
 * delete from debezium where col1 = 2  and if there are 2 records to delete,
 * LogMiner will contain following two statements:
 *
 * delete from "DEBEZIUM" where "ID" = 6 and "COL1" = 2 and "COL2" = 'text' and "COL3" = 'text' and "COL4" IS NULL and "COL5" IS NULL and "COL6" IS NULL and "COL7" IS NULL and "COL8" IS NULL
 * delete from "DEBEZIUM" where "ID" = 7 and "COL1" = 2 and "COL2" = 'text' and "COL3" = 'text' and "COL4" IS NULL and "COL5" IS NULL and "COL6" IS NULL and "COL7" IS NULL and "COL8" IS NULL
 *
 */
public class DeleteParserListener extends BaseDmlStringParserListener {

    DeleteParserListener(final String catalogName, final String schemaName, final OracleDmlParser parser) {
        super(catalogName, schemaName, parser);
    }

    @Override
    protected String getKey(Column column, int index) {
        return column.name();
    }

    @Override
    public void enterDelete_statement(PlSqlParser.Delete_statementContext ctx) {
        init(ctx.general_table_ref().dml_table_expression_clause());
        newColumnValues.clear();
        PlSqlParser.Table_aliasContext tableAlias = ctx.general_table_ref().table_alias();
        alias = tableAlias == null ? "" : tableAlias.getText().toUpperCase();
        PlSqlParser.Where_clauseContext where = ctx.where_clause();
        if (where != null) {
            parseRecursively(ctx.where_clause().expression().logical_expression());
        }
        else {
            oldColumnValues.clear();
        }
        super.enterDelete_statement(ctx);
    }

    @Override
    public void exitDelete_statement(PlSqlParser.Delete_statementContext ctx) {
        List<LogMinerColumnValue> actualOldValues = oldColumnValues.values()
                .stream().map(LogMinerColumnValueWrapper::getColumnValue).collect(Collectors.toList());
        LogMinerDmlEntry newRecord = new LogMinerDmlEntryImpl(Envelope.Operation.DELETE, Collections.emptyList(), actualOldValues);
        parser.setDmlEntry(newRecord);
        super.exitDelete_statement(ctx);
    }
}
