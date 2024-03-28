/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;

/**
 * Parser listener for DROP DATABASE statements.
 *
 * @author Chris Cranford
 */
public class DropDatabaseParserListener extends MySqlParserBaseListener {

    private final MariaDbAntlrDdlParser parser;

    public DropDatabaseParserListener(MariaDbAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterDropDatabase(MySqlParser.DropDatabaseContext ctx) {
        String databaseName = parser.parseName(ctx.uid());
        parser.databaseTables().removeTablesForDatabase(databaseName);
        parser.charsetNameForDatabase().remove(databaseName);
        parser.signalDropDatabase(databaseName, ctx);
        super.enterDropDatabase(ctx);
    }
}
