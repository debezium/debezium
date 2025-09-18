/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.relational.ddl;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.TableId;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public abstract class AbstractDdlParser implements DdlParser {

    protected final boolean skipViews;
    protected final boolean skipComments;
    protected DdlChanges ddlChanges;
    protected SystemVariables systemVariables;

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private String currentSchema = null;
    private static final String BACKTICK_CHARACTER = "`";

    /**
     * Create a new parser.
     *
     * @param includeViews {@code true} if view definitions should be included, or {@code false} if they should be skipped
     * @param includeComments {@code true} if table and column's comment definitions should be included, or {@code false} if they should be skipped
     */
    public AbstractDdlParser(boolean includeViews, boolean includeComments) {
        this.skipViews = !includeViews;
        this.skipComments = !includeComments;
        this.ddlChanges = new DdlChanges();
        this.systemVariables = createNewSystemVariablesInstance();
    }

    @Override
    public void setCurrentSchema(String schemaName) {
        this.currentSchema = schemaName;
    }

    // this parser doesn't distinguish between database name and schema name; what's stored as "database name"
    // in history records is used as "schema" here
    @Override
    public void setCurrentDatabase(String databaseName) {
        this.currentSchema = databaseName;
    }

    @Override
    public DdlChanges getDdlChanges() {
        return ddlChanges;
    }

    @Override
    public SystemVariables systemVariables() {
        return systemVariables;
    }

    protected abstract SystemVariables createNewSystemVariablesInstance();

    /**
     * Get the name of the current schema.
     *
     * @return the current schema name, or null if the current schema name has not been {@link #setCurrentSchema(String) set}
     */
    public String currentSchema() {
        return currentSchema;
    }

    /**
     * Create a {@link TableId} from the supplied schema and table names. By default, this method uses the supplied schema name
     * as the TableId's catalog, which often matches the catalog name in JDBC database metadata.
     *
     * @param schemaName the name of the schema; may be null if not specified
     * @param tableName  the name of the table; should not be null
     * @return the table identifier; never null
     */
    public TableId resolveTableId(String schemaName, String tableName) {
        return new TableId(schemaName, null, tableName);
    }

    /**
     * Determine whether parsing should exclude comments from the token stream. By default, this method returns {@code true}.
     *
     * @return {@code true} if comments should be skipped/excluded, or {@code false} if they should not be skipped
     */
    protected boolean skipComments() {
        return true;
    }

    /**
     * Signal an event to ddl changes listener.
     *
     * @param event the event; may not be null
     */
    protected void signalChangeEvent(DdlParserListener.Event event) {
        this.ddlChanges.handle(event);
    }

    protected void signalSetVariable(String variableName, String variableValue, int order, String statement) {
        signalChangeEvent(new DdlParserListener.SetVariableEvent(variableName, variableValue, currentSchema, order, statement));
    }

    protected void signalUseDatabase(String statement) {
        signalChangeEvent(new DdlParserListener.DatabaseSwitchedEvent(currentSchema, statement));
    }

    /**
     * Signal a create database event to ddl changes listener.
     *
     * @param databaseName the database name; may not be null
     * @param statement    the DDL statement; may not be null
     */
    protected void signalCreateDatabase(String databaseName, String statement) {
        signalChangeEvent(new DdlParserListener.DatabaseCreatedEvent(databaseName, statement));
    }

    /**
     * Signal an alter database event to ddl changes listener.
     *
     * @param databaseName         the database name; may not be null
     * @param previousDatabaseName the previous name of the database if it was renamed, or null if it was not renamed
     * @param statement            the DDL statement; may not be null
     */
    protected void signalAlterDatabase(String databaseName, String previousDatabaseName, String statement) {
        signalChangeEvent(new DdlParserListener.DatabaseAlteredEvent(databaseName, previousDatabaseName, statement));
    }

    /**
     * Signal a drop database event to ddl changes listener.
     *
     * @param databaseName the database name; may not be null
     * @param statement    the DDL statement; may not be null
     */
    protected void signalDropDatabase(String databaseName, String statement) {
        signalChangeEvent(new DdlParserListener.DatabaseCreatedEvent(databaseName, statement));
    }

    /**
     * Signal a create table event to ddl changes listener.
     *
     * @param id        the table identifier; may not be null
     * @param statement the DDL statement; may not be null
     */
    protected void signalCreateTable(TableId id, String statement) {
        signalChangeEvent(new DdlParserListener.TableCreatedEvent(id, statement, false));
    }

    /**
     * Signal an alter table event to ddl changes listener.
     *
     * @param id         the table identifier; may not be null
     * @param previousId the previous name of the view if it was renamed, or null if it was not renamed
     * @param statement  the DDL statement; may not be null
     */
    protected void signalAlterTable(TableId id, TableId previousId, String statement) {
        signalChangeEvent(new DdlParserListener.TableAlteredEvent(id, previousId, statement, false));
    }

    /**
     * Signal a drop table event to ddl changes listener.
     *
     * @param id        the table identifier; may not be null
     * @param statement the statement; may not be null
     */
    protected void signalDropTable(TableId id, String statement) {
        signalChangeEvent(new DdlParserListener.TableDroppedEvent(id, statement, false));
    }

    /**
     * Signal a truncate table event to ddl changes listener.
     *
     * @param id        the table identifier; may not be null
     * @param statement the statement; may not be null
     */
    protected void signalTruncateTable(TableId id, String statement) {
        signalChangeEvent(new DdlParserListener.TableTruncatedEvent(id, statement, false));
    }

    /**
     * Signal a create view event to ddl changes listener.
     *
     * @param id        the table identifier; may not be null
     * @param statement the DDL statement; may not be null
     */
    protected void signalCreateView(TableId id, String statement) {
        signalChangeEvent(new DdlParserListener.TableCreatedEvent(id, statement, true));
    }

    /**
     * Signal an alter view event to ddl changes listener.
     *
     * @param id         the table identifier; may not be null
     * @param previousId the previous name of the view if it was renamed, or null if it was not renamed
     * @param statement  the DDL statement; may not be null
     */
    protected void signalAlterView(TableId id, TableId previousId, String statement) {
        signalChangeEvent(new DdlParserListener.TableAlteredEvent(id, previousId, statement, true));
    }

    /**
     * Signal a drop view event to ddl changes listener.
     *
     * @param id        the table identifier; may not be null
     * @param statement the statement; may not be null
     */
    protected void signalDropView(TableId id, String statement) {
        signalChangeEvent(new DdlParserListener.TableDroppedEvent(id, statement, true));
    }

    /**
     * Signal a create index event to ddl changes listener.
     *
     * @param indexName the name of the index; may not be null
     * @param id        the table identifier; may be null if the index does not apply to a single table
     * @param statement the DDL statement; may not be null
     */
    protected void signalCreateIndex(String indexName, TableId id, String statement) {
        signalChangeEvent(new DdlParserListener.TableIndexCreatedEvent(indexName, id, statement));
    }

    /**
     * Signal a drop index event to ddl changes listener.
     *
     * @param indexName the name of the index; may not be null
     * @param id        the table identifier; may not be null
     * @param statement the DDL statement; may not be null
     */
    protected void signalDropIndex(String indexName, TableId id, String statement) {
        signalChangeEvent(new DdlParserListener.TableIndexDroppedEvent(indexName, id, statement));
    }

    /**
     * Removes line feeds from input string.
     *
     * @param input input with possible line feeds
     * @return input string without line feeds
     */
    protected String removeLineFeeds(String input) {
        return input.replaceAll("[\\n|\\t]", "");
    }

    /**
     * Cut out the string surrounded with single, double and reversed quotes.
     *
     * @param possiblyQuoted string with possible quotes
     * @return string without quotes
     */
    public static String withoutQuotes(String possiblyQuoted) {
        return isQuoted(possiblyQuoted) ? possiblyQuoted.substring(1, possiblyQuoted.length() - 1) : possiblyQuoted;
    }

    /**
     * Check if the string is enclosed in quotes.
     *
     * @param possiblyQuoted string with possible quotes
     * @return true if the string is quoted, false otherwise
     */
    public static boolean isQuoted(String possiblyQuoted) {
        if (possiblyQuoted.length() < 2) {
            // Too short to be quoted ...
            return false;
        }
        if (possiblyQuoted.startsWith("`") && possiblyQuoted.endsWith("`")) {
            return true;
        }
        if (possiblyQuoted.startsWith("'") && possiblyQuoted.endsWith("'")) {
            return true;
        }
        if (possiblyQuoted.startsWith("\"") && possiblyQuoted.endsWith("\"")) {
            return true;
        }
        return false;
    }

    /**
     * Check if the char is quote.
     *
     * @param c possible quote char
     * @return true if the char is quote false otherwise
     */
    public static boolean isQuote(char c) {
        return c == '\'' || c == '"' || c == '`';
    }

    /**
     * Remove the repeated backtick in the middle of the name
     *
     * @param columnName column name
     * @return name without repeated backtick.
     */
    public String removeRepeatedBacktick(String columnName) {
        return columnName.replaceAll(BACKTICK_CHARACTER + BACKTICK_CHARACTER, BACKTICK_CHARACTER);
    }

    /**
     * Utility method to accumulate a parsing exception.
     *
     * @param e    the parsing exception
     * @param list the list of previous parsing exceptions; may be null
     * @return the list of previous and current parsing exceptions; if {@code e} is null then always {@code list}, but otherwise non-null list
     */
    public static Collection<ParsingException> accumulateParsingFailure(ParsingException e, Collection<ParsingException> list) {
        if (e == null) {
            return list;
        }
        if (list == null) {
            list = new ArrayList<ParsingException>();
        }
        list.add(e);
        return list;
    }

    /**
     * Utility method to accumulate a parsing exception.
     *
     * @param e    the multiple parsing exceptions
     * @param list the list of previous parsing exceptions; may be null
     * @return the list of previous and current parsing exceptions; if {@code e} is null then always {@code list}, but otherwise non-null list
     */
    protected Collection<ParsingException> accumulateParsingFailure(MultipleParsingExceptions e, Collection<ParsingException> list) {
        if (e == null) {
            return list;
        }
        if (list == null) {
            list = new ArrayList<ParsingException>();
        }
        list.addAll(e.getErrors());
        return list;
    }

    protected Column createColumnFromConstant(String columnName, String constantValue) {
        ColumnEditor column = Column.editor().name(columnName);
        try {
            if (constantValue.startsWith("'") || constantValue.startsWith("\"")) {
                column.type("CHAR");
                column.jdbcType(Types.CHAR);
                column.length(constantValue.length() - 2);
            }
            else if (constantValue.equalsIgnoreCase("TRUE") || constantValue.equalsIgnoreCase("FALSE")) {
                column.type("BOOLEAN");
                column.jdbcType(Types.BOOLEAN);
            }
            else {
                setTypeInfoForConstant(constantValue, column);
            }
        }
        catch (Throwable t) {
            logger.debug("Unable to create an artificial column for the constant: {}", constantValue);
        }
        return column.create();
    }

    protected void setTypeInfoForConstant(String constantValue, ColumnEditor column) {
        try {
            Integer.parseInt(constantValue);
            column.type("INTEGER");
            column.jdbcType(Types.INTEGER);
        }
        catch (NumberFormatException e) {
        }
        try {
            Long.parseLong(constantValue);
            column.type("BIGINT");
            column.jdbcType(Types.BIGINT);
        }
        catch (NumberFormatException e) {
        }
        try {
            Float.parseFloat(constantValue);
            column.type("FLOAT");
            column.jdbcType(Types.FLOAT);
        }
        catch (NumberFormatException e) {
        }
        try {
            Double.parseDouble(constantValue);
            column.type("DOUBLE");
            column.jdbcType(Types.DOUBLE);
            int precision = 0;
            int scale = 0;
            boolean foundDecimalPoint = false;
            for (int i = 0; i < constantValue.length(); i++) {
                char c = constantValue.charAt(i);
                if (c == '+' || c == '-') {
                    continue;
                }
                else if (c == '.') {
                    foundDecimalPoint = true;
                }
                else if (Character.isDigit(c)) {
                    if (foundDecimalPoint) {
                        ++scale;
                    }
                    else {
                        ++precision;
                    }
                }
                else {
                    break;
                }
            }
            column.length(precision);
            column.scale(scale);
        }
        catch (NumberFormatException e) {
        }
        try {
            BigDecimal decimal = new BigDecimal(constantValue);
            column.type("DECIMAL");
            column.jdbcType(Types.DECIMAL);
            column.length(decimal.precision());
            column.scale(decimal.precision());
        }
        catch (NumberFormatException e) {
        }
    }

    protected void debugParsed(String statement) {
        if (logger.isTraceEnabled()) {
            logger.trace("PARSED:  {}", statement);
        }
    }

    protected void debugSkipped(String statement) {
        if (logger.isTraceEnabled()) {
            logger.trace("SKIPPED: {}", statement);
        }
    }

    protected void commentParsed(String comment) {
        if (logger.isTraceEnabled()) {
            logger.trace("COMMENT: {}", comment);
        }
    }
}
