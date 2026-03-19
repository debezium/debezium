/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;

/**
 * Parser listener that is parsing MySQL DROP DATABASE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class DropDatabaseParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;

    public DropDatabaseParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterDropDatabase(MySqlParser.DropDatabaseContext ctx) {
        String databaseName = parser.parseName(ctx.schemaRef().identifier());
        parser.databaseTables().removeTablesForDatabase(databaseName);
        parser.charsetNameForDatabase().remove(databaseName);
        // Use parent context to include DROP keyword
        parser.signalDropDatabase(databaseName, ctx.getParent());
        super.enterDropDatabase(ctx);
    }
}
