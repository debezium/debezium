/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;

/**
 * This class contains common methods for all listeners
 */
class BaseParserListener extends PlSqlParserBaseListener {

    String getTableName(final PlSqlParser.Tableview_nameContext tableview_name) {
        if (tableview_name.id_expression() != null) {
            return tableview_name.id_expression().getText();
        } else {
            return tableview_name.identifier().id_expression().getText();
        }
    }

    String getColumnName(final PlSqlParser.Column_nameContext ctx) {
        return ctx.identifier().id_expression().getText();
    }
}
