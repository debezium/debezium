/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import static io.debezium.connector.oracle.antlr.listener.ParserUtils.getTableName;

import java.util.LinkedHashMap;
import java.util.Map;

import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.OracleChangeRecordValueConverter;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueImpl;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueWrapper;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.text.ParsingException;

/**
 * This class contains common methods for DML parser listeners
 */
abstract class BaseDmlParserListener<T> extends PlSqlParserBaseListener {

    protected String catalogName;
    protected String schemaName;
    protected Table table;
    final OracleChangeRecordValueConverter converter;
    String alias;

    protected OracleDmlParser parser;

    Map<T, LogMinerColumnValueWrapper> newColumnValues = new LinkedHashMap<>();
    Map<T, LogMinerColumnValueWrapper> oldColumnValues = new LinkedHashMap<>();

    BaseDmlParserListener(String catalogName, String schemaName, OracleDmlParser parser) {
        this.parser = parser;
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.converter = parser.getConverters();
    }

    // Defines the key of the Map of LogMinerColumnValueWrapper. It could be String or Integer
    abstract protected T getKey(Column column, int index);

    /**
     * This method prepares all column value placeholders, based on the table metadata
     * @param ctx DML table expression context
     */
    void init(PlSqlParser.Dml_table_expression_clauseContext ctx) {
        String tableName = getTableName(ctx.tableview_name());
        table = parser.databaseTables().forTable(catalogName, schemaName, tableName);
        if (table == null) {
            throw new ParsingException(null, "Trying to parse a table, which does not exist.");
        }
        for (int i = 0; i < table.columns().size(); i++) {
            Column column = table.columns().get(i);
            int type = column.jdbcType();
            T key = getKey(column, i);
            String name = ParserUtils.stripeQuotes(column.name().toUpperCase());
            newColumnValues.put(key, new LogMinerColumnValueWrapper(new LogMinerColumnValueImpl(name, type)));
            oldColumnValues.put(key, new LogMinerColumnValueWrapper(new LogMinerColumnValueImpl(name, type)));
        }
    }
}
