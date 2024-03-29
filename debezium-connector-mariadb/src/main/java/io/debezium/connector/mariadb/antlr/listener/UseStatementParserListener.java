/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables.BinlogScope;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParserBaseListener;

/**
 * Parser listener that parses USE statements.<p></p>
 *
 * USE statements alter the current database/catalog for all changes that come thereafter.
 *
 * @author Chris Cranford
 */
public class UseStatementParserListener extends MariaDBParserBaseListener {

    private final MariaDbAntlrDdlParser parser;

    public UseStatementParserListener(MariaDbAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterUseStatement(MariaDBParser.UseStatementContext ctx) {
        String dbName = parser.parseName(ctx.uid());
        parser.setCurrentSchema(dbName);

        // Every time the database switches to a different database, it sets the "character_set_database" and
        // "collation_database" system variables. We replicate that behavior here (or the variable we care about)
        // so that these variables are always right for the current database.
        String charsetForDb = parser.charsetNameForDatabase().get(dbName);
        parser.systemVariables().setVariable(BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_DATABASE, charsetForDb);

        // Signal that the variable was set ...
        parser.signalUseDatabase(ctx);
        super.enterUseStatement(ctx);
    }
}
