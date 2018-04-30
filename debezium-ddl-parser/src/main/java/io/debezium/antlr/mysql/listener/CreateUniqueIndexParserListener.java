/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr.mysql.listener;

import io.debezium.antlr.mysql.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;

import static io.debezium.antlr.AntlrDdlParser.getText;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class CreateUniqueIndexParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parserCtx;


    public CreateUniqueIndexParserListener(MySqlAntlrDdlParser parserCtx) {
        this.parserCtx = parserCtx;
    }

    @Override
    public void enterCreateIndex(MySqlParser.CreateIndexContext ctx) {
        if (ctx.UNIQUE() != null) {
            TableId tableId = parserCtx.parseQualifiedTableId(ctx.tableName().fullId());
            TableEditor tableEditor = parserCtx.databaseTables().editTable(tableId);
            if (tableEditor != null) {
                if (!tableEditor.hasPrimaryKey()) {
                    parserCtx.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
                }
            }
            else {
                throw new ParsingException(null, "Trying to create index on non existing table " + parserCtx.getFullTableName(tableId) + "."
                        + "Query: " + getText(ctx));
            }
        }
        // TODO rkuchar fixed together with MySql legacy parser bug.
        parserCtx.signalAlterTable(null, null, ctx);
        super.enterCreateIndex(ctx);
    }
}
