/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueWrapper;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;

/**
 * This class parses recursively logical expression tree for DELETE and UPDATE statements
 */
abstract class BaseDmlStringParserListener extends BaseDmlParserListener<String> {

    boolean isUpdate;

    BaseDmlStringParserListener(String catalogName, String schemaName, OracleDmlParser parser) {
        super(catalogName, schemaName, parser);
    }

    @Override
    public void enterTable_alias(PlSqlParser.Table_aliasContext ctx) {
        alias = ctx.getText().toUpperCase();
    }

    /**
     * Logical expressions are trees and (column name, value) pairs are nested in this tree.
     * This methods extracts those pairs and store them in List<LogMinerColumnValue> oldValues
     * This method is used by VALUES parsers of update and delete statements.
     * @param logicalExpression expression tree
     */
    void parseRecursively(PlSqlParser.Logical_expressionContext logicalExpression) {

        int count = logicalExpression.logical_expression().size();
        if (count == 0) {

            String nullValue = logicalExpression.getStop().getText();

            String expression = logicalExpression.getText();
            String columnName = "";
            String value = "";
            if (expression.contains("=")) {
                columnName = expression.substring(0, expression.indexOf("=")).toUpperCase();
                value = expression.substring(expression.indexOf("=") + 1);
            }
            if ("null".equalsIgnoreCase(nullValue)) {
                columnName = expression.substring(0, expression.toUpperCase().indexOf("ISNULL")).toUpperCase();
                value = nullValue;
            }

            columnName = ParserUtils.stripeAlias(columnName, alias);
            columnName = ParserUtils.stripeQuotes(columnName);

            Column column = table.columnWithName(columnName);
            Object stripedValue = ParserUtils.removeApostrophes(value);

            LogMinerColumnValueWrapper logMinerColumnValueWrapper = oldColumnValues.get(columnName);
            if (logMinerColumnValueWrapper != null) { // todo this used to happen for ROWID pseudo column. Test if this is not a problem after NO_ROWID_IN_STMT option
                Object valueObject = ParserUtils.convertValueToSchemaType(column, stripedValue, converter);
                logMinerColumnValueWrapper.setProcessed(true);
                logMinerColumnValueWrapper.getColumnValue().setColumnData(valueObject);
            }

        }
        for (int i = 0; i < count; i++) {
            parseRecursively(logicalExpression.logical_expression(i));
        }
    }

}
