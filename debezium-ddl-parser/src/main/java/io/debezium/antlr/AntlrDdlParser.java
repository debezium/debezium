/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.AbstractDdlParser;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.Collection;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public abstract class AntlrDdlParser<L extends Lexer, P extends Parser> extends AbstractDdlParser {

    private boolean throwErrorsFromTreeWalk;
    private AntlrDdlParserListener antlrDdlParserListener;

    protected Tables databaseTables;
    protected DataTypeResolver dataTypeResolver = new DataTypeResolver();

    public AntlrDdlParser(boolean throwErrorsFromTreeWalk) {
        this(throwErrorsFromTreeWalk, false);
    }

    public AntlrDdlParser(boolean throwErrorsFromTreeWalk, boolean includeViews) {
        super(";", includeViews);
        this.throwErrorsFromTreeWalk = throwErrorsFromTreeWalk;
    }

    @Override
    public void parse(String ddlContent, Tables databaseTables) {
        this.databaseTables = databaseTables;

        CodePointCharStream ddlContentCharStream = CharStreams.fromString(ddlContent);
        L lexer = createNewLexerInstance(new CaseChangingCharStream(ddlContentCharStream, isGrammarInUpperCase()));
        P parser = createNewParserInstance(new CommonTokenStream(lexer));

        initDataTypes(dataTypeResolver);

        // remove default console output printing error listener
        parser.removeErrorListener(ConsoleErrorListener.INSTANCE);

        ParsingErrorListener parsingErrorListener = new ParsingErrorListener(AbstractDdlParser::accumulateParsingFailure);
        parser.addErrorListener(parsingErrorListener);

        ParseTree parseTree = parseTree(parser);

        if (parsingErrorListener.getErrors().isEmpty()) {
            antlrDdlParserListener = createParseTreeWalkerListener();
            ParseTreeWalker.DEFAULT.walk(antlrDdlParserListener, parseTree);

            if (throwErrorsFromTreeWalk && !antlrDdlParserListener.getErrors().isEmpty()) {
                throwParsingException(antlrDdlParserListener.getErrors());
            }
        }
        else {
            throwParsingException(parsingErrorListener.getErrors());
        }
    }

    public Collection<ParsingException> getParsingExceptionsFromWalker() {
        return antlrDdlParserListener.getErrors();
    }

    /**
     * Examine the supplied string containing DDL statements, and apply those statements to the specified
     * database table definitions.
     *
     * @param parser initialized ANTLR parser instance with common token stream from DDL statement; may not be null
     */
    protected abstract ParseTree parseTree(P parser);

    protected abstract AntlrDdlParserListener createParseTreeWalkerListener();

    /**
     * Creates a new generic type instance of ANTLR Lexer.
     *
     * @param charStreams the char stream from DDL statement, without one line comments and line feeds; may not be null
     * @return new instance of generic ANTLR Lexer
     */
    protected abstract L createNewLexerInstance(CharStream charStreams);

    /**
     * Creates a new generic type instance of ANTLR Parser.
     *
     * @param commonTokenStream the stream of ANTLR tokens created from Lexer instance; may not be null
     * @return new instance of generic ANTLR Parser
     */
    protected abstract P createNewParserInstance(CommonTokenStream commonTokenStream);

    /**
     * Check if the parsed grammar is written in upper case.
     *
     * @return true if grammar is written in upper case; false if in lower case
     */
    protected abstract boolean isGrammarInUpperCase();

    /**
     * Initialize DB to JDBC data types mapping for resolver.
     *
     * @param dataTypeResolver data type resolver
     */
    protected abstract void initDataTypes(DataTypeResolver dataTypeResolver);

    public Tables databaseTables() {
        return databaseTables;
    }

    public DataTypeResolver dataTypeResolver() {
        return dataTypeResolver;
    }

    /**
     * Returns matched part of the getText for the context.
     *
     * @param ctx the parser rule context; may not be null
     * @return matched part of the getText
     */
    public static String getText(ParserRuleContext ctx) {
        Interval interval = new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex());
        return ctx.start.getInputStream().getText(interval);
    }

    public boolean skipViews() {
        return skipViews;
    }

    public void signalSetVariable(String variableName, String variableValue, ParserRuleContext ctx) {
        signalSetVariable(variableName, variableValue, getText(ctx));
    }

    /**
     * Signal a create database event to ddl changes listener.
     *
     * @param databaseName the database name; may not be null
     * @param ctx          the start of the statement; may not be null
     */
    public void signalCreateDatabase(String databaseName, ParserRuleContext ctx) {
        signalCreateDatabase(databaseName, getText(ctx));
    }

    /**
     * Signal an alter database event to ddl changes listener.
     *
     * @param databaseName         the database name; may not be null
     * @param previousDatabaseName the previous name of the database if it was renamed, or null if it was not renamed
     * @param ctx                  the start of the statement; may not be null
     */
    public void signalAlterDatabase(String databaseName, String previousDatabaseName, ParserRuleContext ctx) {
        signalAlterDatabase(databaseName, previousDatabaseName, getText(ctx));
    }

    /**
     * Signal a drop database event to ddl changes listener.
     *
     * @param databaseName the database name; may not be null
     * @param ctx          the start of the statement; may not be null
     */
    public void signalDropDatabase(String databaseName, ParserRuleContext ctx) {
        signalDropDatabase(databaseName, getText(ctx));
    }

    /**
     * Signal a create table event to ddl changes listener.
     *
     * @param id  the table identifier; may not be null
     * @param ctx the start of the statement; may not be null
     */
    public void signalCreateTable(TableId id, ParserRuleContext ctx) {
        signalCreateTable(id, getText(ctx));
    }

    /**
     * Signal an alter table event to ddl changes listener.
     *
     * @param id         the table identifier; may not be null
     * @param previousId the previous name of the view if it was renamed, or null if it was not renamed
     * @param ctx        the start of the statement; may not be null
     */
    public void signalAlterTable(TableId id, TableId previousId, ParserRuleContext ctx) {
        signalAlterTable(id, previousId, getText(ctx));
    }

    @Override
    public void signalDropTable(TableId id, String statement) {
        super.signalDropTable(id, statement);
    }

    /**
     * Signal a drop table event to ddl changes listener.
     *
     * @param id  the table identifier; may not be null
     * @param ctx the start of the statement; may not be null
     */
    public void signalDropTable(TableId id, ParserRuleContext ctx) {
        signalDropTable(id, getText(ctx));
    }

    /**
     * Signal a truncate table event to ddl changes listener.
     *
     * @param id  the table identifier; may not be null
     * @param ctx the start of the statement; may not be null
     */
    public void signalTruncateTable(TableId id, ParserRuleContext ctx) {
        signalTruncateTable(id, getText(ctx));
    }

    /**
     * Signal a create view event to ddl changes listener.
     *
     * @param id  the table identifier; may not be null
     * @param ctx the start of the statement; may not be null
     */
    public void signalCreateView(TableId id, ParserRuleContext ctx) {
        signalCreateView(id, getText(ctx));
    }

    /**
     * Signal an alter view event to ddl changes listener.
     *
     * @param id         the table identifier; may not be null
     * @param previousId the previous name of the view if it was renamed, or null if it was not renamed
     * @param ctx        the start of the statement; may not be null
     */
    public void signalAlterView(TableId id, TableId previousId, ParserRuleContext ctx) {
        signalAlterView(id, previousId, getText(ctx));
    }

    /**
     * Signal a drop view event to ddl changes listener.
     *
     * @param id  the table identifier; may not be null
     * @param ctx the start of the statement; may not be null
     */
    public void signalDropView(TableId id, ParserRuleContext ctx) {
        signalDropView(id, getText(ctx));
    }

    /**
     * Signal a create index event to ddl changes listener.
     *
     * @param indexName the name of the index; may not be null
     * @param id        the table identifier; may be null if the index does not apply to a single table
     * @param ctx       the start of the statement; may not be null
     */
    public void signalCreateIndex(String indexName, TableId id, ParserRuleContext ctx) {
        signalCreateIndex(indexName, id, getText(ctx));
    }

    /**
     * Signal a drop index event to ddl changes listener.
     *
     * @param indexName the name of the index; may not be null
     * @param id        the table identifier; may not be null
     * @param ctx       the start of the statement; may not be null
     */
    public void signalDropIndex(String indexName, TableId id, ParserRuleContext ctx) {
        signalDropIndex(indexName, id, getText(ctx));
    }

    public void debugParsed(ParserRuleContext ctx) {
        debugParsed(getText(ctx));
    }

    public void debugSkipped(ParserRuleContext ctx) {
        debugSkipped(getText(ctx));
    }

    public String withoutQuotes(ParserRuleContext ctx) {
        return withoutQuotes(ctx.getText());
    }

    private void throwParsingException(Collection<ParsingException> errors) {
        if(errors.size() == 1) {
            throw errors.iterator().next();
        }
        else {
            throw new MultipleParsingExceptions(errors);
        }
    }
}
