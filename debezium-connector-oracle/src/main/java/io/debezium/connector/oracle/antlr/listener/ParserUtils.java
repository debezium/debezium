/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.oracle.logminer.OracleChangeRecordValueConverter;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueWrapper;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.ValueConverter;

/**
 * This class contains a few methods for parser listeners
 */
public class ParserUtils {

    private ParserUtils() {
    }

    /**
     * This method returns pure column name without quotes
     * @param ctx column name context
     * @return column name
     */
    public static String getColumnName(final PlSqlParser.Column_nameContext ctx) {
        return stripeQuotes(ctx.identifier().id_expression().getText());
    }

    /**
     * stripes double quotes that surrounds a string
     * @param text text
     * @return text without surrounding double quotes
     */
    public static String stripeQuotes(String text) {
        if (text != null && text.indexOf("\"") == 0 && text.lastIndexOf("\"") == text.length() - 1) {
            return text.substring(1, text.length() - 1);
        }
        return text;
    }

    /**
     * this method stripes table alias and dot give string
     * @param text string with possible alias
     * @param alias table alias
     * @return striped string
     */
    public static String stripeAlias(String text, String alias) {
        int index = text.indexOf(alias + ".");
        if (index >= 0) {
            return text.substring(alias.length() + 1);
        }
        return text;
    }

    /**
     * Initialize new column values with old column values.
     * It does not override new values which were processed already in where clause parsing
     * @param newColumnValues values to set or insert
     * @param oldColumnValues values in WHERE clause
     * @param table Debezium Table object
     */
    public static void cloneOldToNewColumnValues(Map<String, LogMinerColumnValueWrapper> newColumnValues, Map<String, LogMinerColumnValueWrapper> oldColumnValues,
                                                 Table table) {
        for (Column column : table.columns()) {
            final LogMinerColumnValueWrapper newColumnValue = newColumnValues.get(column.name());
            if (!newColumnValue.isProcessed()) {
                final LogMinerColumnValueWrapper oldColumnValue = oldColumnValues.get(column.name());
                newColumnValue.setProcessed(true);
                newColumnValue.getColumnValue().setColumnData(oldColumnValue.getColumnValue().getColumnData());
            }
        }
    }

    /**
     * This converts the given value to the appropriate object. The conversion is based on the column definition
     *
     * @param column column Object
     * @param value value object
     * @param converters given converter
     * @return object as the result of this conversion. It could be null if converter cannot build the schema
     * or if converter or value are null
     */
    public static Object convertValueToSchemaType(Column column, Object value, OracleChangeRecordValueConverter converters) {
        if (converters != null && value != null) {
            final SchemaBuilder schemaBuilder = converters.schemaBuilder(column);
            if (schemaBuilder == null) {
                return null;
            }
            final Schema schema = schemaBuilder.build();
            final Field field = new Field(column.name(), 1, schema);
            final ValueConverter valueConverter = converters.converter(column, field);

            return valueConverter.convert(value);
        }
        return null;
    }

    /**
     * In some cases values of the parsed expression are enclosed in apostrophes.
     * Even null values are surrounded by single apostrophes. This method removes them.
     *
     * @param text supplied value which might be enclosed by apostrophes.
     * @return clean String or null in case if test = "null" or = "NULL"
     */
    public static String removeApostrophes(String text) {
        if (text != null && text.indexOf("'") == 0 && text.lastIndexOf("'") == text.length() - 1) {
            return text.substring(1, text.length() - 1);
        }
        if ("null".equalsIgnoreCase(text)) {
            return null;
        }
        return text;
    }

    /**
     * this is to handle cases when a record contains escape character(s)
     * @param text before parsing we replaced it with double escape, now revert it back
     * @return string with double slashes
     */
    public static String replaceDoubleBackSlashes(String text) {
        if (text != null && text.contains("\\\\")) {
            return text.replaceAll("\\\\\\\\", "\\\\");
        }
        return text;
    }

    /**
     * Obtains the table name
     * @param tableview_name table view context
     * @return table name
     */
    static String getTableName(final PlSqlParser.Tableview_nameContext tableview_name) {
        if (tableview_name.id_expression() != null) {
            return stripeQuotes(tableview_name.id_expression().getText());
        }
        else {
            return stripeQuotes(tableview_name.identifier().id_expression().getText());
        }
    }
}
