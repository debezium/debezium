/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr.mysql.listener;

import io.debezium.antlr.mysql.MySqlAntlrDdlParser;
import io.debezium.antlr.mysql.MySqlSystemVariables;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class UseStatementParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parserCtx;

    public UseStatementParserListener(MySqlAntlrDdlParser parserCtx) {
        this.parserCtx = parserCtx;
    }

    @Override
    public void enterUseStatement(MySqlParser.UseStatementContext ctx) {
        String dbName = parserCtx.parseName(ctx.uid());
        parserCtx.setCurrentSchema(dbName);

        // Every time MySQL switches to a different database, it sets the "character_set_database" and "collation_database"
        // system variables. We replicate that behavior here (or the variable we care about) so that these variables are always
        // right for the current database.
        String charsetForDb = parserCtx.charsetNameForDatabase().get(dbName);
        parserCtx.systemVariables().setVariable(MySqlSystemVariables.MySqlScope.SESSION, MySqlSystemVariables.CHARSET_NAME_DATABASE, charsetForDb);
        super.enterUseStatement(ctx);
    }
}
