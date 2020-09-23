/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jsqlparser;

import java.io.StringReader;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.antlr.listener.ParserUtils;
import io.debezium.connector.oracle.logminer.OracleChangeRecordValueConverter;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueImpl;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueWrapper;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntryImpl;
import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.text.ParsingException;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

/**
 * This class does parsing of simple DML: insert, update, delete.
 * Log Miner supplies very simple syntax , that this parser should be sufficient to parse those.
 * It does no support joins, merge, sub-selects and other complicated cases, which should be OK for Log Miner case
 */
public class SimpleDmlParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleDmlParser.class);
    protected final String catalogName;
    protected final String schemaName;
    private final OracleChangeRecordValueConverter converter;
    private final CCJSqlParserManager pm;
    private final Map<String, LogMinerColumnValueWrapper> newColumnValues = new LinkedHashMap<>();
    private final Map<String, LogMinerColumnValueWrapper> oldColumnValues = new LinkedHashMap<>();
    protected Table table;
    private String aliasName;

    /**
     * Constructor
     * @param catalogName database name
     * @param schemaName user name
     * @param converter value converter
     */
    public SimpleDmlParser(String catalogName, String schemaName, OracleChangeRecordValueConverter converter) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.converter = converter;
        pm = new CCJSqlParserManager();
    }

    /**
     * This parses a DML
     * @param dmlContent DML
     * @param tables debezium Tables
     * @return parsed value holder class
     */
    public LogMinerDmlEntry parse(String dmlContent, Tables tables, String txId) {
        try {

            // If a table contains Spatial data type, DML input generates two entries in REDO LOG.
            // First with actual statement and second with NULL. It is not relevant at this point
            if (dmlContent == null) {
                LOGGER.debug("Cannot parse NULL , transaction: {}", txId);
                return null;
            }
            // todo investigate: happens on CTAS
            if (dmlContent.endsWith(";null;")) {
                dmlContent = dmlContent.substring(0, dmlContent.lastIndexOf(";null;"));
            }
            if (!dmlContent.endsWith(";")) {
                dmlContent = dmlContent + ";";
            }
            // this is to handle cases when a record contains escape character(s). This parser throws.
            dmlContent = dmlContent.replaceAll("\\\\", "\\\\\\\\");
            dmlContent = dmlContent.replaceAll("= Unsupported Type", "= null"); // todo address spatial data types

            newColumnValues.clear();
            oldColumnValues.clear();

            Statement st = pm.parse(new StringReader(dmlContent));
            if (st instanceof Update) {
                parseUpdate(tables, (Update) st);
                List<LogMinerColumnValue> actualNewValues = newColumnValues.values().stream()
                        .filter(LogMinerColumnValueWrapper::isProcessed).map(LogMinerColumnValueWrapper::getColumnValue).collect(Collectors.toList());
                List<LogMinerColumnValue> actualOldValues = oldColumnValues.values().stream()
                        .filter(LogMinerColumnValueWrapper::isProcessed).map(LogMinerColumnValueWrapper::getColumnValue).collect(Collectors.toList());
                return new LogMinerDmlEntryImpl(Envelope.Operation.UPDATE, actualNewValues, actualOldValues);

            }
            else if (st instanceof Insert) {
                parseInsert(tables, (Insert) st);
                List<LogMinerColumnValue> actualNewValues = newColumnValues.values()
                        .stream().map(LogMinerColumnValueWrapper::getColumnValue).collect(Collectors.toList());
                return new LogMinerDmlEntryImpl(Envelope.Operation.CREATE, actualNewValues, Collections.emptyList());

            }
            else if (st instanceof Delete) {
                parseDelete(tables, (Delete) st);
                List<LogMinerColumnValue> actualOldValues = oldColumnValues.values()
                        .stream().map(LogMinerColumnValueWrapper::getColumnValue).collect(Collectors.toList());
                return new LogMinerDmlEntryImpl(Envelope.Operation.DELETE, Collections.emptyList(), actualOldValues);

            }
            else {
                LOGGER.error("Operation {} is not supported yet", st);
                return null;
            }

        }
        catch (Throwable e) {
            LOGGER.error("Cannot parse statement : {}, transaction: {}, due to the {}", dmlContent, txId, e);
            return null;
        }

    }

    private void initColumns(Tables tables, String tableName) {
        table = tables.forTable(catalogName, schemaName, tableName);
        if (table == null) {
            TableId id = new TableId(catalogName, schemaName, tableName);
            throw new ParsingException(null, "Trying to parse a table '" + id + "', which does not exist.");
        }
        for (int i = 0; i < table.columns().size(); i++) {
            Column column = table.columns().get(i);
            int type = column.jdbcType();
            String key = column.name();
            String name = ParserUtils.stripeQuotes(column.name().toUpperCase());
            newColumnValues.put(key, new LogMinerColumnValueWrapper(new LogMinerColumnValueImpl(name, type)));
            oldColumnValues.put(key, new LogMinerColumnValueWrapper(new LogMinerColumnValueImpl(name, type)));
        }
    }

    // this parses simple statement with only one table
    private void parseUpdate(Tables tables, Update st) throws JSQLParserException {
        int tableCount = st.getTables().size();
        if (tableCount > 1 || tableCount == 0) {
            throw new JSQLParserException("DML includes " + tableCount + " tables");
        }
        net.sf.jsqlparser.schema.Table parseTable = st.getTables().get(0);
        initColumns(tables, ParserUtils.stripeQuotes(parseTable.getName()));

        List<net.sf.jsqlparser.schema.Column> columns = st.getColumns();
        Alias alias = parseTable.getAlias();
        aliasName = alias == null ? "" : alias.getName().trim();

        List<Expression> expressions = st.getExpressions(); // new values
        setNewValues(expressions, columns);
        Expression where = st.getWhere(); // old values
        if (where != null) {
            parseWhereClause(where);
            ParserUtils.cloneOldToNewColumnValues(newColumnValues, oldColumnValues, table);
        }
        else {
            oldColumnValues.clear();
        }
    }

    private void parseInsert(Tables tables, Insert st) {
        initColumns(tables, ParserUtils.stripeQuotes(st.getTable().getName()));
        Alias alias = st.getTable().getAlias();
        aliasName = alias == null ? "" : alias.getName().trim();

        List<net.sf.jsqlparser.schema.Column> columns = st.getColumns();
        ItemsList values = st.getItemsList();
        values.accept(new ItemsListVisitorAdapter() {
            @Override
            public void visit(ExpressionList expressionList) {
                super.visit(expressionList);
                List<Expression> expressions = expressionList.getExpressions();
                setNewValues(expressions, columns);
            }
        });
        oldColumnValues.clear();
    }

    private void parseDelete(Tables tables, Delete st) {
        initColumns(tables, ParserUtils.stripeQuotes(st.getTable().getName()));
        Alias alias = st.getTable().getAlias();
        aliasName = alias == null ? "" : alias.getName().trim();

        newColumnValues.clear();

        Expression where = st.getWhere();
        if (where != null) {
            parseWhereClause(where);
        }
        else {
            oldColumnValues.clear();
        }
    }

    private void setNewValues(List<Expression> expressions, List<net.sf.jsqlparser.schema.Column> columns) {
        if (expressions.size() != columns.size()) {
            throw new RuntimeException("DML has " + expressions.size() + " column values, but Table object has " + columns.size() + " columns");
        }

        for (int i = 0; i < columns.size(); i++) {
            String columnName = ParserUtils.stripeQuotes(columns.get(i).getColumnName().toUpperCase());
            String value = ParserUtils.stripeQuotes(expressions.get(i).toString());
            Object stripedValue = ParserUtils.removeApostrophes(value);
            Column column = table.columnWithName(columnName);
            if (column == null) {
                LOGGER.trace("excluded column: {}", columnName);
                continue;
            }
            Object valueObject = ParserUtils.convertValueToSchemaType(column, stripedValue, converter);

            LogMinerColumnValueWrapper logMinerColumnValueWrapper = newColumnValues.get(columnName);
            if (logMinerColumnValueWrapper != null) {
                logMinerColumnValueWrapper.setProcessed(true);
                logMinerColumnValueWrapper.getColumnValue().setColumnData(valueObject);
            }
        }
    }

    private void parseWhereClause(Expression logicalExpression) {

        logicalExpression.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(EqualsTo expr) {
                super.visit(expr);
                String columnName = expr.getLeftExpression().toString();
                columnName = ParserUtils.stripeAlias(columnName, aliasName);
                String value = expr.getRightExpression().toString();
                columnName = ParserUtils.stripeQuotes(columnName);

                Column column = table.columnWithName(columnName);
                if (column == null) {
                    LOGGER.trace("excluded column in where clause: {}", columnName);
                    return;
                }
                value = ParserUtils.removeApostrophes(value);

                LogMinerColumnValueWrapper logMinerColumnValueWrapper = oldColumnValues.get(columnName.toUpperCase());
                if (logMinerColumnValueWrapper != null) {
                    Object valueObject = ParserUtils.convertValueToSchemaType(column, value, converter);
                    logMinerColumnValueWrapper.setProcessed(true);
                    logMinerColumnValueWrapper.getColumnValue().setColumnData(valueObject);
                }
            }

            @Override
            public void visit(IsNullExpression expr) {
                super.visit(expr);
                String columnName = expr.getLeftExpression().toString();
                columnName = ParserUtils.stripeAlias(columnName, aliasName);
                columnName = ParserUtils.stripeQuotes(columnName);
                Column column = table.columnWithName(columnName);
                if (column == null) {
                    LOGGER.trace("excluded column in where clause: {}", columnName);
                    return;
                }
                LogMinerColumnValueWrapper logMinerColumnValueWrapper = oldColumnValues.get(columnName.toUpperCase());
                if (logMinerColumnValueWrapper != null) {
                    logMinerColumnValueWrapper.setProcessed(true);
                    logMinerColumnValueWrapper.getColumnValue().setColumnData(null);
                }
            }
        });
    }
}
