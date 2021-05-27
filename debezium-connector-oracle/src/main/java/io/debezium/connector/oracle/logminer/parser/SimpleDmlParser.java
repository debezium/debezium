/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.antlr.listener.ParserUtils;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueImpl;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueWrapper;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntryImpl;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
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
 * LogMiner supplies very simple syntax , that this parser should be sufficient to parse those.
 * It does no support joins, merge, sub-selects and other complicated cases, which should be OK for LogMiner case
 */
public class SimpleDmlParser implements DmlParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleDmlParser.class);
    protected final String catalogName;
    private final OracleValueConverters converter;
    private final CCJSqlParserManager pm;
    private final Map<String, LogMinerColumnValueWrapper> newColumnValues = new LinkedHashMap<>();
    private final Map<String, LogMinerColumnValueWrapper> oldColumnValues = new LinkedHashMap<>();
    protected Table table;
    private String aliasName;

    /**
     * Constructor
     * @param catalogName database name
     * @param converter value converter
     */
    public SimpleDmlParser(String catalogName, OracleValueConverters converter) {
        this.catalogName = catalogName;
        this.converter = converter;
        pm = new CCJSqlParserManager();
    }

    /**
     * This parses a DML
     * @param dmlContent DML
     * @param table the table
     * @return parsed value holder class
     */
    @Override
    public LogMinerDmlEntry parse(String dmlContent, Table table, String txId) {
        try {

            // If a table contains Spatial data type, DML input generates two entries in REDO LOG.
            // First with actual statement and second with NULL. It is not relevant at this point
            if (dmlContent == null) {
                LOGGER.debug("Cannot parse NULL , transaction: {}", txId);
                return null;
            }
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
                parseUpdate(table, (Update) st);
                Object[] actualNewValues = getColumnValueArray(newColumnValues, false);
                Object[] actualOldValues = getColumnValueArray(oldColumnValues, false);
                return LogMinerDmlEntryImpl.forUpdate(actualNewValues, actualOldValues);

            }
            else if (st instanceof Insert) {
                parseInsert(table, (Insert) st);
                Object[] actualNewValues = getColumnValueArray(newColumnValues, false);
                return LogMinerDmlEntryImpl.forInsert(actualNewValues);

            }
            else if (st instanceof Delete) {
                parseDelete(table, (Delete) st);
                Object[] actualOldValues = getColumnValueArray(oldColumnValues, false);
                return LogMinerDmlEntryImpl.forDelete(actualOldValues);
            }
            else {
                throw new DmlParserException("Unexpected DML operation not supported");
            }
        }
        catch (Throwable e) {
            throw new DmlParserException("Cannot parse DML: " + dmlContent, e);
        }
    }

    private static Object[] getColumnValueArray(Map<String, LogMinerColumnValueWrapper> map, boolean processed) {
        // Internally this parser still uses the LogMinerColumnValue and LogMinerColumnValueWrapper classes as
        // intermediate storage objects. Since this parser will be removed in the near future, it seemed
        // appropriate to only bridge the changes when creating the LogMinerDmlEntry rather than adjust the
        // inner workings of this parser entirely.
        Stream<LogMinerColumnValueWrapper> stream = map.values().stream();
        if (processed) {
            stream = stream.filter(LogMinerColumnValueWrapper::isProcessed);
        }
        return stream.map(LogMinerColumnValueWrapper::getColumnValue)
                .map(LogMinerColumnValue::getColumnData)
                .toArray();
    }

    private void initColumns(Table table, String tableName) {
        if (!table.id().table().equals(tableName)) {
            throw new ParsingException(null, "Resolved TableId expected table name '" + table.id().table() + "' but is '" + tableName + "'");
        }
        this.table = table;
        for (int i = 0; i < table.columns().size(); i++) {
            Column column = table.columns().get(i);
            String key = column.name();
            String name = ParserUtils.stripeQuotes(column.name().toUpperCase());
            newColumnValues.put(key, new LogMinerColumnValueWrapper(new LogMinerColumnValueImpl(name)));
            oldColumnValues.put(key, new LogMinerColumnValueWrapper(new LogMinerColumnValueImpl(name)));
        }
    }

    // this parses simple statement with only one table
    private void parseUpdate(Table table, Update st) throws JSQLParserException {
        int tableCount = st.getTables().size();
        if (tableCount > 1 || tableCount == 0) {
            throw new JSQLParserException("DML includes " + tableCount + " tables");
        }
        net.sf.jsqlparser.schema.Table parseTable = st.getTables().get(0);
        initColumns(table, ParserUtils.stripeQuotes(parseTable.getName()));

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

    private void parseInsert(Table table, Insert st) {
        initColumns(table, ParserUtils.stripeQuotes(st.getTable().getName()));
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

    private void parseDelete(Table table, Delete st) {
        initColumns(table, ParserUtils.stripeQuotes(st.getTable().getName()));
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
