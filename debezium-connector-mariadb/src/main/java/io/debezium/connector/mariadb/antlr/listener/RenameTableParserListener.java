/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableId;

/**
 * Parser listener for parsing RENAME TABLE statements.
 *
 * @author Chris Cranford
 */
public class RenameTableParserListener extends MySqlParserBaseListener {

    private final static Logger LOG = LoggerFactory.getLogger(RenameTableParserListener.class);

    private final MariaDbAntlrDdlParser parser;

    public RenameTableParserListener(MariaDbAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterRenameTableClause(MySqlParser.RenameTableClauseContext ctx) {
        TableId oldTable = parser.parseQualifiedTableId(ctx.tableName(0).fullId());
        TableId newTable = parser.parseQualifiedTableId(ctx.tableName(1).fullId());
        if (parser.getTableFilter().isIncluded(oldTable) && !parser.getTableFilter().isIncluded(newTable)) {
            LOG.warn("Renaming included table {} to non-included table {}, this can lead to schema inconsistency", oldTable, newTable);
        }
        else if (!parser.getTableFilter().isIncluded(oldTable) && parser.getTableFilter().isIncluded(newTable)) {
            LOG.warn("Renaming non-included table {} to included table {}, this can lead to schema inconsistency", oldTable, newTable);
        }
        parser.databaseTables().renameTable(oldTable, newTable);
        parser.signalAlterTable(newTable, oldTable, ctx);
        super.enterRenameTableClause(ctx);
    }
}
