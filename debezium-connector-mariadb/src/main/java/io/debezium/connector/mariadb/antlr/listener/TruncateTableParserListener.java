/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParserBaseListener;
import io.debezium.relational.TableId;

/**
 * Parser listener that parses TRUNCATE TABLE statements.
 *
 * @author Chris Cranford
 */
public class TruncateTableParserListener extends MariaDBParserBaseListener {

    private final MariaDbAntlrDdlParser parser;

    public TruncateTableParserListener(MariaDbAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterTruncateTable(MariaDBParser.TruncateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());

        // Be aware the legacy parser is not signaling truncate events
        parser.signalTruncateTable(tableId, ctx);
        super.enterTruncateTable(ctx);
    }
}
