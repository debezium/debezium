/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr;

import io.debezium.relational.Tables;
import io.debezium.relational.ddl.AbstractDdlParser;
import io.debezium.text.MultipleParsingExceptions;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public abstract class AntlrDdlParser<L extends Lexer, P extends Parser> extends AbstractDdlParser {

    public AntlrDdlParser() {
        super(";");
    }

    @Override
    public void parse(String ddlContent, Tables databaseTables) {

        L lexer = getLexerNewInstance(CharStreams.fromString(removeLineFeeds(ddlContent)));
        P parser = getParserNewInstance(new CommonTokenStream(lexer));

        // remove base output printing error listener
        parser.removeErrorListener(ConsoleErrorListener.INSTANCE);

        ParsingErrorListener parsingErrorListener = new ParsingErrorListener(this::accumulateParsingFailure);
        parser.addErrorListener(parsingErrorListener);

        startParsing(parser, databaseTables);

        if (parsingErrorListener.getErrors().size() > 0) {
            throw new MultipleParsingExceptions(parsingErrorListener.getErrors());
        }
    }

    protected abstract void startParsing(P parser, Tables databaseTables);

    protected abstract L getLexerNewInstance(CodePointCharStream charStreams);

    protected abstract P getParserNewInstance(CommonTokenStream commonTokenStream);
}
