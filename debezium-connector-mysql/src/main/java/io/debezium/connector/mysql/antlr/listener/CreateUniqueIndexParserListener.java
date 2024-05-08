/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import static io.debezium.antlr.AntlrDdlParser.getText;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;

/**
 * Parser listener that is parsing MySQL CREATE UNIQUE INDEX statements, that will be used as a primary key
 * if it's not already defined for the table.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class CreateUniqueIndexParserListener extends MySqlParserBaseListener {

    private final static Logger LOG = LoggerFactory.getLogger(AlterTableParserListener.class);

    private final MySqlAntlrDdlParser parser;

    public CreateUniqueIndexParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterCreateIndex(MySqlParser.CreateIndexContext ctx) {
        if (ctx.UNIQUE() != null) {
            TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
            if (!parser.getTableFilter().isIncluded(tableId)) {
                LOG.debug("{} is not monitored, no need to process unique index", tableId);
                return;
            }
            TableEditor tableEditor = parser.databaseTables().editTable(tableId);
            if (tableEditor != null) {
                if (!tableEditor.hasPrimaryKey() && parser.isTableUniqueIndexIncluded(ctx.indexColumnNames(), tableEditor)) {
                    parser.parseUniqueIndexColumnNames(ctx.indexColumnNames(), tableEditor);
                    parser.databaseTables().overwriteTable(tableEditor.create());
                    parser.signalCreateIndex(parser.parseName(ctx.uid()), tableId, ctx);
                }
            }
            else {
                throw new ParsingException(null, "Trying to create index on non existing table " + tableId.toString() + "."
                        + "Query: " + getText(ctx));
            }
        }
        super.enterCreateIndex(ctx);
    }
}
