/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.text.Position;
import io.debezium.text.TokenStream;
import io.debezium.text.TokenStream.Marker;

/**
 * A parser for DDL statements.
 * 
 * @author Randall Hauch
 * @author Horia Chiorean
 * @author Barry LaFond
 */
@NotThreadSafe
public class DdlParser {

    protected static interface TokenSet {
        void add(String token);

        default void add(String firstToken, String... additionalTokens) {
            add(firstToken);
            for (String token : additionalTokens)
                add(token);
        }
    }

    private final Set<String> keywords = new HashSet<>();
    private final Set<String> statementStarts = new HashSet<>();
    private final String terminator;
    private String currentSchema = null;
    protected final boolean skipViews;
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final DataTypeParser dataTypeParser = new DataTypeParser();
    protected Tables databaseTables;
    protected TokenStream tokens;

    /**
     * Create a new parser that uses the supplied {@link DataTypeParser}, but that does not include view definitions.
     * 
     * @param terminator the terminator character sequence; may be null if the default terminator ({@code ;}) should be used
     */
    public DdlParser(String terminator) {
        this(terminator,false);
    }

    /**
     * Create a new parser that uses the supplied {@link DataTypeParser}.
     * 
     * @param terminator the terminator character sequence; may be null if the default terminator ({@code ;}) should be used
     * @param includeViews {@code true} if view definitions should be included, or {@code false} if they should be skipped
     */
    public DdlParser(String terminator, boolean includeViews) {
        this.terminator = terminator != null ? terminator : ";";
        this.skipViews = !includeViews;
        initializeDataTypes(dataTypeParser);
        initializeKeywords(keywords::add);
        initializeStatementStarts(statementStarts::add);
    }

    protected void initializeDataTypes(DataTypeParser dataTypeParser) {
    }

    protected void initializeKeywords(TokenSet keywords) {
    }

    protected void initializeStatementStarts(TokenSet statementStartTokens) {
        statementStartTokens.add("CREATE", "ALTER", "DROP", "INSERT", "SET", "GRANT", "REVOKE");
    }

    protected final String terminator() {
        return terminator;
    }

    protected int determineTokenType(int type, String token) {
        if (statementStarts.contains(token)) type |= DdlTokenizer.STATEMENT_KEY;
        if (keywords.contains(token)) type |= DdlTokenizer.KEYWORD;
        if (terminator.equals(token)) type |= DdlTokenizer.STATEMENT_TERMINATOR;
        return type;
    }

    /**
     * Set the name of the current schema used when {@link #resolveTableId(String, String) resolving} {@link TableId}s.
     * 
     * @param name the name of the current schema; may be null
     */
    public void setCurrentSchema(String name) {
        this.currentSchema = name;
    }

    /**
     * Get the name of the current schema.
     * 
     * @return the current schema name, or null if the current schema name has not been {@link #setCurrentSchema(String) set}
     */
    public String currentSchema() {
        return currentSchema;
    }

    /**
     * Parse the next tokens for a possibly qualified name. This method builds up a string containing the schema name (or if none
     * is found the the {@link #currentSchema() current schema}), a '.' delimiter, and the object name. If no schema name is
     * found, just the object name is returned.
     * 
     * @param start the start of the statement
     * @return the qualified schema name.
     */
    protected String parseSchemaQualifiedName(Marker start) {
        String first = tokens.consume();
        if (tokens.canConsume('.')) {
            String second = tokens.consume();
            return first + "." + second;
        }
        if (currentSchema() != null) {
            return currentSchema() + "." + first;
        }
        return first;
    }

    /**
     * Parse the next tokens for a possibly qualified table name. This method uses the schema name that appears in the
     * token stream, or if none is found the {@link #currentSchema()}, and then calls {@link #resolveTableId(String, String)} with
     * the values.
     * 
     * @param start the start of the statement
     * @return the resolved {@link TableId}
     */
    protected TableId parseQualifiedTableName(Marker start) {
        String name = tokens.consume();
        if (tokens.canConsume('.')) {
            String tableName = tokens.consume();
            return resolveTableId(name, tableName);
        }
        return resolveTableId(currentSchema(), name);
    }

    /**
     * Create a {@link TableId} from the supplied schema and table names. By default, this method uses the supplied schema name
     * as the TableId's catalog, which often matches the catalog name in JDBC database metadata.
     * 
     * @param schemaName the name of the schema; may be null if not specified
     * @param tableName the name of the table; should not be null
     * @return the table identifier; never null
     */
    protected TableId resolveTableId(String schemaName, String tableName) {
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
     * Examine the supplied string containing DDL statements, and apply those statements to the specified
     * database table definitions.
     * 
     * @param ddlContent the stream of tokens containing the DDL statements; may not be null
     * @param databaseTables the database's table definitions, which should be used by this method to create, change, or remove
     *            tables as defined in the DDL content; may not be null
     * @throws ParsingException if there is a problem parsing the supplied content
     */
    public final void parse(String ddlContent, Tables databaseTables) {
        TokenStream stream = new TokenStream(ddlContent, new DdlTokenizer(!skipComments(), this::determineTokenType), false);
        stream.start();
        parse(stream, databaseTables);
    }

    /**
     * Examine the stream starting at its current position for DDL statements, and apply those statements to the specified
     * database table definitions.
     * 
     * @param ddlContent the stream of tokens containing the DDL statements; may not be null
     * @param databaseTables the database's table definitions, which should be used by this method to create, change, or remove
     *            tables as defined in the DDL content; may not be null
     * @throws ParsingException if there is a problem parsing the supplied content
     * @throws IllegalStateException if the supplied token stream is in an invalid state
     */
    public final void parse(TokenStream ddlContent, Tables databaseTables) throws ParsingException, IllegalStateException {
        this.tokens = ddlContent;
        this.databaseTables = databaseTables;
        Marker marker = ddlContent.mark();
        try {
            while (ddlContent.hasNext()) {
                parseNextStatement(ddlContent.mark());
                // Consume the statement terminator if it is still there ...
                tokens.canConsume(DdlTokenizer.STATEMENT_TERMINATOR);
            }
        } catch (ParsingException e) {
            ddlContent.rewind(marker);
            throw e;
        } catch (Throwable t) {
            parsingFailed(ddlContent.nextPosition(), "Unexpected exception (" + t.getMessage() + ") parsing", t);
        }
    }

    /**
     * Parse the next DDL statement. This is the primary entry point for subclasses.
     * 
     * @param marker the start of the statement; never null
     * @throws ParsingException if there is an error parsing the statement
     */
    protected void parseNextStatement(Marker marker) {
        if (tokens.matches(DdlTokenizer.COMMENT)) {
            parseComment(marker);
        } else if (tokens.matches("CREATE")) {
            parseCreate(marker);
        } else if (tokens.matches("ALTER")) {
            parseAlter(marker);
        } else if (tokens.matches("DROP")) {
            parseDrop(marker);
        } else {
            parseUnknownStatement(marker);
        }
    }

    /**
     * Parse the a DDL line comment. This is generally called by {@link #parseNextStatement(Marker)} for line comments that appear
     * between other DDL statements, and is not typically called for comments that appear <i>within</i> DDL statements.
     * 
     * @param marker the start of the statement; never null
     * @throws ParsingException if there is an error parsing the statement
     */
    protected void parseComment(Marker marker) {
        String comment = tokens.consume();
        logger.trace("COMMENT: {}", comment);
    }

    /**
     * Parse the a DDL "CREATE" statement. This method is intended to be overridden by subclasses.
     * <p>
     * By default this method simply consumes the complete statement.
     * 
     * @param marker the start of the statement; never null
     * @throws ParsingException if there is an error parsing the statement
     */
    protected void parseCreate(Marker marker) {
        consumeStatement();
    }

    /**
     * Parse the a DDL "ALTER" statement. This method is intended to be overridden by subclasses.
     * <p>
     * By default this method simply consumes the complete statement.
     * 
     * @param marker the start of the statement; never null
     * @throws ParsingException if there is an error parsing the statement
     */
    protected void parseAlter(Marker marker) {
        consumeStatement();
    }

    /**
     * Parse the a DDL "DROP" statement. This method is intended to be overridden by subclasses.
     * <p>
     * By default this method simply consumes the complete statement.
     * 
     * @param marker the start of the statement; never null
     * @throws ParsingException if there is an error parsing the statement
     */
    protected void parseDrop(Marker marker) {
        consumeStatement();
    }

    /**
     * Parse a DDL statement that is not known by the {@link #parseNextStatement}. This method can be overridden by subclasses,
     * although it will be more common for subclasses to override {@link #parseNextStatement}.
     * <p>
     * By default this method simply consumes the complete statement.
     * 
     * @param marker the start of the statement; never null
     * @throws ParsingException if there is an error parsing the statement
     */
    protected void parseUnknownStatement(Marker marker) {
        consumeStatement();
    }

    protected void debugParsed(Marker statementStart) {
        if (logger.isTraceEnabled()) {
            String statement = removeLineFeeds(tokens.getContentFrom(statementStart));
            logger.trace("PARSED:  {}", statement);
        }
    }

    protected void debugSkipped(Marker statementStart) {
        if (logger.isTraceEnabled()) {
            String statement = removeLineFeeds(tokens.getContentFrom(statementStart));
            logger.trace("SKIPPED: {}", statement);
        }
    }

    private String removeLineFeeds(String input) {
        return input.replaceAll("[\\n|\\t]", "");
    }

    /**
     * Consume all tokens from the current position that is a {@link #initializeStatementStarts(TokenSet) starting-statement
     * token} until either the
     * {@link #terminator() end-of-statement terminator token} or before the next
     * {@link #initializeStatementStarts(TokenSet) starting-statement token}.
     * 
     * @throws ParsingException if the next token is not a {@link #initializeStatementStarts(TokenSet) starting-statement token}
     */
    protected void consumeStatement() throws ParsingException {
        Marker start = tokens.mark();
        tokens.consume(DdlTokenizer.STATEMENT_KEY);
        consumeRemainingStatement(start);
    }

    /**
     * Consume all tokens from the current position until and including either the {@link #terminator() end-of-statement
     * terminator token} or one of
     * the {@link #initializeStatementStarts(TokenSet) tokens that is registered} as the start of a statement.
     * 
     * @param start the marker at which the statement was begun
     */
    protected void consumeRemainingStatement(Marker start) {
        while (tokens.hasNext()) {
            if (tokens.matches(DdlTokenizer.STATEMENT_KEY)) break;
            if (tokens.canConsume("BEGIN")) {
                tokens.consumeThrough("END");
            } else if (tokens.matches(DdlTokenizer.STATEMENT_TERMINATOR)) {
                tokens.consume();
                break;
            }
            tokens.consume();
        }
    }

    /**
     * Consume the next token that is a single-quoted string.
     * 
     * @return the quoted string; never null
     * @throws ParsingException if there is no single-quoted string at the current position
     */
    protected String consumeSingleQuotedString() {
        return tokens.consumeAnyOf(DdlTokenizer.SINGLE_QUOTED_STRING);
    }

    /**
     * Consume the next token that is a double-quoted string.
     * 
     * @return the quoted string; never null
     * @throws ParsingException if there is no double-quoted string at the current position
     */
    protected String consumeDoubleQuotedString() {
        return tokens.consumeAnyOf(DdlTokenizer.DOUBLE_QUOTED_STRING);
    }

    /**
     * Consume the next token that is either a single-quoted string or a double-quoted string.
     * 
     * @return the quoted string; never null
     * @throws ParsingException if there is no single- or double-quoted string at the current position
     */
    protected String consumeQuotedString() {
        return tokens.consumeAnyOf(DdlTokenizer.SINGLE_QUOTED_STRING, DdlTokenizer.DOUBLE_QUOTED_STRING);
    }

    /**
     * Generate a {@link ParsingException} with the supplied message, which is appended by this method with additional
     * information about the position's line and column.
     * 
     * @param position the position at which the error occurred; may not be null
     * @param msg the leading portion of the message; may not be null
     */
    protected void parsingFailed(Position position, String msg) {
        parsingFailed(position, msg);
    }

    /**
     * Generate a {@link ParsingException} with the supplied message, which is appended by this method with additional
     * information about the position's line and column.
     * 
     * @param position the position at which the error occurred; may not be null
     * @param msg the leading portion of the message; may not be null
     * @param t the exception that occurred; may be null
     */
    protected void parsingFailed(Position position, String msg, Throwable t) {
        throw new ParsingException(position, msg + " at line " + position.line() + ", column " + position.column(), t);
    }

    /**
     * Generate a {@link ParsingException} or {@link MultipleParsingExceptions} with the supplied error or errors and the
     * supplied message, which is appended by this method with additional information about the position's line and column.
     * 
     * @param position the position at which the error occurred; may not be null
     * @param errors the multiple parsing exception errors; may not be null
     * @param msg the leading portion of the message; may not be null
     */
    protected void parsingFailed(Position position, Collection<ParsingException> errors, String msg) {
        if (errors == null || errors.isEmpty()) {
            throw new ParsingException(position, msg + " at line " + position.line() + ", column " + position.column());
        }
        throw new MultipleParsingExceptions(msg + " at line " + position.line() + ", column " + position.column(), errors);
    }

    protected Object parseLiteral(Marker start) {
        if (tokens.canConsume('_')) { // introducer
            // This is a character literal beginning with a character set ...
            parseCharacterSetName(start);
            return parseCharacterLiteral(start);
        }
        if (tokens.canConsume('N')) {
            return parseCharacterLiteral(start);
        }
        if (tokens.canConsume("U", "&")) {
            return parseCharacterLiteral(start);
        }
        if (tokens.canConsume('X')) {
            return parseCharacterLiteral(start);
        }
        if (tokens.matchesAnyOf(DdlTokenizer.DOUBLE_QUOTED_STRING, DdlTokenizer.SINGLE_QUOTED_STRING)) {
            return tokens.consume();
        }
        if (tokens.canConsume("DATE")) {
            return parseDateLiteral(start);
        }
        if (tokens.canConsume("TIME")) {
            return parseDateLiteral(start);
        }
        if (tokens.canConsume("TIMESTAMP")) {
            return parseDateLiteral(start);
        }
        if (tokens.canConsume("TRUE")) {
            return Boolean.TRUE;
        }
        if (tokens.canConsume("FALSE")) {
            return Boolean.FALSE;
        }
        if (tokens.canConsume("UNKNOWN")) {
            return Boolean.FALSE;
        }
        // Otherwise, it's just a numeric literal ...
        return parseNumericLiteral(start, true);
    }

    protected Object parseNumericLiteral(Marker start, boolean signed) {
        StringBuilder sb = new StringBuilder();
        boolean decimal = false;
        if (signed && tokens.matches("+", "-")) {
            sb.append(tokens.consumeAnyOf("+", "-"));
        }
        if (!tokens.canConsume('.')) {
            sb.append(tokens.consumeInteger());
        }
        if (tokens.canConsume('.')) {
            sb.append(tokens.consumeInteger());
            decimal = true;
        }
        if (!tokens.canConsume('E')) {
            if (decimal) return Double.parseDouble(sb.toString());
            return Integer.parseInt(sb.toString());
        }
        sb.append('E');
        if (tokens.matches("+", "-")) {
            sb.append(tokens.consumeAnyOf("+", "-"));
        }
        sb.append(tokens.consumeInteger());
        return new BigDecimal(sb.toString());
    }

    protected String parseCharacterLiteral(Marker start) {
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (tokens.matches(DdlTokenizer.COMMENT)) {
                parseComment(start);
            } else if (tokens.matchesAnyOf(DdlTokenizer.SINGLE_QUOTED_STRING, DdlTokenizer.DOUBLE_QUOTED_STRING)) {
                if (sb.length() != 0) sb.append(' ');
                sb.append(tokens.consume());
            } else {
                break;
            }
        }
        if (tokens.canConsume("ESCAPE")) {
            tokens.consume();
        }
        return sb.toString();
    }

    protected String parseCharacterSetName(Marker start) {
        String name = tokens.consume();
        if (tokens.canConsume('.')) {
            // The name was actually a schema name ...
            String id = tokens.consume();
            return name + "." + id;
        }
        return name;
    }

    protected String parseDateLiteral(Marker start) {
        return consumeQuotedString();
    }

    protected String parseTimeLiteral(Marker start) {
        return consumeQuotedString();
    }

    protected String parseTimestampLiteral(Marker start) {
        return consumeQuotedString();
    }

    /**
     * Parse the column information in the SELECT clause. This statement stops before consuming the FROM clause.
     * 
     * @param start the start of the statement
     * @return the map of resolved Columns keyed by the column alias (or name) used in the SELECT statement; never null but
     *         possibly
     *         empty if we couldn't parse the SELECT clause correctly
     */
    protected Map<String, Column> parseColumnsInSelectClause(Marker start) {
        // Parse the column names ...
        Map<String, String> tableAliasByColumnAlias = new LinkedHashMap<>();
        Map<String, String> columnNameByAliases = new LinkedHashMap<>();
        parseColumnName(start, tableAliasByColumnAlias, columnNameByAliases);
        while (tokens.canConsume(',')) {
            parseColumnName(start, tableAliasByColumnAlias, columnNameByAliases);
        }

        // Parse the FROM clause, but we'll back up to the start of this before we return ...
        Marker startOfFrom = tokens.mark();
        Map<String, Column> columnsByName = new LinkedHashMap<>();
        Map<String, Table> fromTablesByAlias = parseSelectFromClause(start);
        Table singleTable = fromTablesByAlias.size() == 1 ? fromTablesByAlias.values().stream().findFirst().get() : null;
        tableAliasByColumnAlias.forEach((columnAlias, tableAlias) -> {
            // Resolve the alias into the actual column name in the referenced table ...
            String columnName = columnNameByAliases.getOrDefault(columnAlias, columnAlias);
            Column column = null;
            if (tableAlias == null) {
                // The column was not qualified with a table, so there should be a single table ...
                column = singleTable == null ? null : singleTable.columnWithName(columnName);
            } else {
                // The column was qualified with a table, so look it up ...
                Table table = fromTablesByAlias.get(tableAlias);
                column = table == null ? null : table.columnWithName(columnName);
            }
            if (column == null) {
                // Check to see whether the column name contains a constant value, in which case we need to create an
                // artificial column ...
                column = createColumnFromConstant(columnAlias, columnName);
            }
            columnsByName.put(columnAlias, column); // column may be null
        });
        tokens.rewind(startOfFrom);
        return columnsByName;
    }

    protected Column createColumnFromConstant(String columnName, String constantValue) {
        ColumnEditor column = Column.editor().name(columnName);
        try {
            if (constantValue.startsWith("'") || constantValue.startsWith("\"")) {
                column.typeName("CHAR");
                column.jdbcType(Types.CHAR);
                column.length(constantValue.length() - 2);
            } else if (constantValue.equalsIgnoreCase("TRUE") || constantValue.equalsIgnoreCase("FALSE")) {
                column.typeName("BOOLEAN");
                column.jdbcType(Types.BOOLEAN);
            } else {
                setTypeInfoForConstant(constantValue, column);
            }
        } catch (Throwable t) {
            logger.debug("Unable to create an artificial column for the constant: " + constantValue);
        }
        return column.create();
    }

    protected void setTypeInfoForConstant(String constantValue, ColumnEditor column) {
        try {
            Integer.parseInt(constantValue);
            column.typeName("INTEGER");
            column.jdbcType(Types.INTEGER);
        } catch (NumberFormatException e) {
        }
        try {
            Long.parseLong(constantValue);
            column.typeName("BIGINT");
            column.jdbcType(Types.BIGINT);
        } catch (NumberFormatException e) {
        }
        try {
            Float.parseFloat(constantValue);
            column.typeName("FLOAT");
            column.jdbcType(Types.FLOAT);
        } catch (NumberFormatException e) {
        }
        try {
            Double.parseDouble(constantValue);
            column.typeName("DOUBLE");
            column.jdbcType(Types.DOUBLE);
            int precision = 0;
            int scale = 0;
            boolean foundDecimalPoint = false;
            for (int i = 0; i < constantValue.length(); i++) {
                char c = constantValue.charAt(i);
                if (c == '+' || c == '-') {
                    continue;
                } else if (c == '.') {
                    foundDecimalPoint = true;
                } else if ( Character.isDigit(c) ) {
                    if ( foundDecimalPoint ) ++scale;
                    else ++precision;
                } else {
                    break;
                }
            }
            column.length(precision);
            column.scale(scale);
        } catch (NumberFormatException e) {
        }
        try {
            BigDecimal decimal = new BigDecimal(constantValue);
            column.typeName("DECIMAL");
            column.jdbcType(Types.DECIMAL);
            column.length(decimal.precision());
            column.scale(decimal.precision());
        } catch (NumberFormatException e) {
        }
    }

    protected String determineTypeNameForConstant(long value) {
        return "BIGINT";
    }

    protected String determineTypeNameForConstant(float value) {
        return "FLOAT";
    }

    protected String determineTypeNameForConstant(double value) {
        return "DECIMAL";
    }

    protected String determineTypeNameForConstant(BigDecimal value) {
        return "BIGINT";
    }

    /**
     * Parse the potentially qualified and aliased column information, and add the information to the supplied maps.
     * 
     * @param start the start of the statement
     * @param tableAliasByColumnAliases the map to which is added the column's alias (or name) keyed by the alias of the table
     *            in which the column should appear; may not be null
     * @param columnNameByAliases the map to which is added the column's name keyed by the its alias (or itself if there is no
     *            alias); may not be null
     */
    protected void parseColumnName(Marker start, Map<String, String> tableAliasByColumnAliases, Map<String, String> columnNameByAliases) {
        try {
            String tableName = tokens.consume();
            String columnName = null;
            if (tokens.canConsume('.')) {
                columnName = tokens.consume();
            } else {
                // Just an unqualified column name ...
                columnName = tableName;
                tableName = null;
            }
            String alias = columnName;
            if (tokens.canConsume("AS")) {
                alias = tokens.consume();
            }
            columnNameByAliases.put(alias, columnName);
            tableAliasByColumnAliases.put(alias, tableName);
        } catch (ParsingException e) {
            // do nothing, and don't rewind ...
        }
    }

    /**
     * Returns the tables keyed by their aliases that appear in a SELECT clause's "FROM" list. This method handles the
     * {@link #canConsumeJoin(Marker) various standard joins}.
     * 
     * @param start the start of the statement
     * @return the map of resolved tables keyed by the alias (or table name) used in the SELECT statement; never null but possibly
     *         empty if we couldn't parse the from clause correctly
     */
    protected Map<String, Table> parseSelectFromClause(Marker start) {
        Map<String, Table> tablesByAlias = new HashMap<>();
        if (tokens.canConsume("FROM")) {
            try {
                parseAliasedTableInFrom(start, tablesByAlias);
                while (tokens.canConsume(',') || canConsumeJoin(start)) {
                    parseAliasedTableInFrom(start, tablesByAlias);
                    canConsumeJoinCondition(start);
                }
            } catch (ParsingException e) {
                // do nothing ...
            }
        }
        return tablesByAlias;
    }

    protected boolean canConsumeJoin(Marker start) {
        return tokens.canConsume("JOIN") ||
                tokens.canConsume("INNER", "JOIN") ||
                tokens.canConsume("OUTER", "JOIN") ||
                tokens.canConsume("CROSS", "JOIN") ||
                tokens.canConsume("RIGHT", "OUTER", "JOIN") ||
                tokens.canConsume("LEFT", "OUTER", "JOIN") ||
                tokens.canConsume("FULL", "OUTER", "JOIN");
    }

    protected boolean canConsumeJoinCondition(Marker start) {
        if (tokens.canConsume("ON")) {
            try {
                parseSchemaQualifiedName(start);
                while (tokens.canConsume(DdlTokenizer.SYMBOL)) {
                }
                parseSchemaQualifiedName(start);
                return true;
            } catch (ParsingException e) {
                // do nothing
            }
        }
        return false;

    }

    /**
     * Parse a potentially qualified table name along with an optional alias.
     * 
     * @param start the start of the statement
     * @param tablesByAlias the map to which this method should add the table keyed by its alias (or name if there is no alias);
     *            may not be null
     */
    private void parseAliasedTableInFrom(Marker start, Map<String, Table> tablesByAlias) {
        Table fromTable = databaseTables.forTable(parseQualifiedTableName(start));
        // Aliases in JOIN clauses don't have to be preceded by AS, but can simply be the alias followed by the 'ON' clause
        if (tokens.matches("AS", TokenStream.ANY_VALUE, "ON") || tokens.matches(TokenStream.ANY_VALUE, "ON")) {
            tokens.canConsume("AS");
            String alias = tokens.consume();
            if (fromTable != null) {
                tablesByAlias.put(alias, fromTable);
                return;
            }
        }
        if (fromTable != null) tablesByAlias.put(fromTable.id().table(), fromTable);
    }

}
