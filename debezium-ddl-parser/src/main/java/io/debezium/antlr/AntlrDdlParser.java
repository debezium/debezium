/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr;

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

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public abstract class AntlrDdlParser<L extends Lexer, P extends Parser> extends AbstractDdlParser {

    public AntlrDdlParser() {
        super(";");
    }

    @Override
    public void parse(String ddlContent, Tables databaseTables) {

        CodePointCharStream ddlContentCharStream = CharStreams.fromString(removeLineFeeds(replaceOneLineComments(ddlContent)));
        L lexer = createNewLexerInstance(new CaseChangingCharStream(ddlContentCharStream, isGrammarInUpperCase()));
        P parser = createNewParserInstance(new CommonTokenStream(lexer));

        // remove default console output printing error listener
        parser.removeErrorListener(ConsoleErrorListener.INSTANCE);

        ParsingErrorListener parsingErrorListener = new ParsingErrorListener(this::accumulateParsingFailure);
        parser.addErrorListener(parsingErrorListener);

        parse(parser, databaseTables);

        if (parsingErrorListener.getErrors().size() > 0) {
            throw new MultipleParsingExceptions(parsingErrorListener.getErrors());
        }
    }

    /**
     * Examine the supplied string containing DDL statements, and apply those statements to the specified
     * database table definitions.
     *
     * @param parser         initialized ANTLR parser instance with common token stream from DDL statement; may not be null
     * @param databaseTables the database's table definitions, which should be used by this method to create, change, or remove
     *                       tables as defined in the DDL content; may not be null
     * @throws ParsingException if there is a problem parsing the supplied content
     */
    protected abstract void parse(P parser, Tables databaseTables);

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
     * Replace one line comment syntax by multiline syntax.
     *
     * @param statement statement with one line comments; may not be null
     * @return statement without one line syntax comments
     */
    protected abstract String replaceOneLineComments(String statement);

    /**
     * Returns parsed ddl statement.
     *
     * @param ctx the parser rule context which matches the whole ddl statement; may not be null
     * @return parsed ddl statement.
     */
    protected String statement(ParserRuleContext ctx) {
        Interval interval = new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex());
        return ctx.start.getInputStream().getText(interval);
    }
}
