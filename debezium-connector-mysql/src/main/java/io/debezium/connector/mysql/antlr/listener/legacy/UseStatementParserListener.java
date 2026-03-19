/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener.legacy;

import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.mysql.antlr.MySqlPtAntlrDdlParser;
import io.debezium.ddl.parser.mysql.legacy.MySqlParser;
import io.debezium.ddl.parser.mysql.legacy.MySqlParserBaseListener;

/**
 * Parser listener that is parsing MySQL USE statements that changes
 * current database/schema on which all changes are applied.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class UseStatementParserListener extends MySqlParserBaseListener {

    private final MySqlPtAntlrDdlParser parser;

    public UseStatementParserListener(MySqlPtAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterUseStatement(MySqlParser.UseStatementContext ctx) {
        String dbName = parser.parseName(ctx.uid());
        parser.setCurrentSchema(dbName);

        // Every time MySQL switches to a different database, it sets the "character_set_database" and "collation_database"
        // system variables. We replicate that behavior here (or the variable we care about) so that these variables are always
        // right for the current database.
        String charsetForDb = parser.charsetNameForDatabase().get(dbName);
        parser.systemVariables().setVariable(BinlogSystemVariables.BinlogScope.SESSION, BinlogSystemVariables.CHARSET_NAME_DATABASE, charsetForDb);

        // Signal that the variable was set ...
        parser.signalUseDatabase(ctx);
        super.enterUseStatement(ctx);
    }
}
