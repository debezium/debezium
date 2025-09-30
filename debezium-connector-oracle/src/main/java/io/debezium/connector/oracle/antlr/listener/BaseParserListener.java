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
        final String tableName;
        if (tableview_name.id_expression() != null) {
            tableName = tableview_name.id_expression().getText();
        }
        else {
            tableName = tableview_name.identifier().id_expression().getText();
        }
        return getTableOrColumnName(tableName);
    }

    String getTableName(final PlSqlParser.Table_nameContext table_name) {
        if (table_name.identifier() != null) {
            return getTableOrColumnName(table_name.identifier().getText());
        }
        return getTableOrColumnName(table_name.getText());
    }

    String getTableName(final PlSqlParser.Column_nameContext ctx) {
        final String tableName;
        if (ctx.id_expression() != null && ctx.id_expression().size() > 1) {
            tableName = getTableOrColumnName(ctx.id_expression(0).getText());
        }
        else {
            tableName = getTableOrColumnName(ctx.identifier().id_expression().getText());
        }
        return tableName;
    }

    String getColumnName(final PlSqlParser.Column_nameContext ctx) {
        final String columnName;
        if (ctx.id_expression() != null && ctx.id_expression().size() > 0) {
            columnName = getTableOrColumnName(ctx.id_expression(ctx.id_expression().size() - 1).getText());
        }
        else {
            columnName = getTableOrColumnName(ctx.identifier().id_expression().getText());
        }
        return columnName;
    }

    String getColumnName(final PlSqlParser.Old_column_nameContext ctx) {
        return getTableOrColumnName(ctx.getText());
    }

    String getColumnName(final PlSqlParser.New_column_nameContext ctx) {
        return getTableOrColumnName(ctx.getText());
    }

    /**
     * Resolves a table or column name from the provided string.
     *
     * Oracle table and column names are inherently stored in upper-case; however, if the objects
     * are created using double-quotes, the case of the object name is retained.  Therefore when
     * needing to parse a table or column name, this method will adhere to those rules and will
     * always return the name in upper-case unless the provided name is double-quoted in which
     * the returned value will have the double-quotes removed and case retained.
     *
     * @param name table or column name
     * @return parsed table or column name from the supplied name argument
     */
    private static String getTableOrColumnName(String name) {
        return removeQuotes(name, true);
    }

    /**
     * Removes leading and trailing double quote characters from the provided string.
     *
     * @param text value to have double quotes removed
     * @param upperCaseIfNotQuoted control if returned string is upper-cased if not quoted
     * @return string that has had quotes removed
     */
    @SuppressWarnings("SameParameterValue")
    private static String removeQuotes(String text, boolean upperCaseIfNotQuoted) {
        if (text != null && text.length() > 2 && text.startsWith("\"") && text.endsWith("\"")) {
            return text.substring(1, text.length() - 1);
        }
        return upperCaseIfNotQuoted ? text.toUpperCase() : text;
    }
}
