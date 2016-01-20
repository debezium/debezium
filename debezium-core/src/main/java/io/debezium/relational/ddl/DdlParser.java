/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
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
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final DataTypeParser dataTypeParser = new DataTypeParser();
    protected Tables databaseTables;
    protected TokenStream tokens;

    /**
     * Create a new parser that uses the supplied {@link DataTypeParser}.
     * 
     * @param terminator the terminator character sequence; may be null if the default terminator ({@code ;}) should be used
     */
    public DdlParser(String terminator) {
        this.terminator = terminator != null ? terminator : ";";
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
    protected void setCurrentSchema(String name) {
        this.currentSchema = name;
    }

    /**
     * Get the name of the current schema.
     * 
     * @return the current schema name, or null if the current schema name has not been {@link #setCurrentSchema(String) set}
     */
    protected String currentSchema() {
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
        TokenStream stream = new TokenStream(ddlContent, new DdlTokenizer(!skipComments(), this::determineTokenType), true);
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
        logger.debug("COMMENT: {}", comment);
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
        if (logger.isDebugEnabled()) {
            String statement = removeLineFeeds(tokens.getContentFrom(statementStart));
            logger.debug("PARSED:  {}", statement);
        }
    }

    protected void debugSkipped(Marker statementStart) {
        if (logger.isDebugEnabled()) {
            String statement = removeLineFeeds(tokens.getContentFrom(statementStart));
            logger.debug("SKIPPED: {}", statement);
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
            if (tokens.matches(DdlTokenizer.STATEMENT_TERMINATOR)) {
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
        parsingFailed(position, null, msg);
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
}
