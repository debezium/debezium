/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.antlr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.BiFunction;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import io.debezium.text.ParsingException;
import io.debezium.text.Position;
import io.debezium.util.Loggings;

/**
 * ANTLR parsing error listener.
 *
 * This listener will collect all errors, which may appear during a construction of parsed tree.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class ParsingErrorListener extends BaseErrorListener {

    private Collection<ParsingException> errors = new ArrayList<>();
    private final BiFunction<ParsingException, Collection<ParsingException>, Collection<ParsingException>> accumulateError;
    private final String parsedDdl;

    public ParsingErrorListener(String parsedDdl, BiFunction<ParsingException, Collection<ParsingException>, Collection<ParsingException>> accumulateError) {
        this.accumulateError = accumulateError;
        this.parsedDdl = parsedDdl;
    }

    /**
     * Parsing error listener, which will throw up an exception.
     */
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
        final String location = "DDL statement couldn't be parsed at line " + line + ", position " + charPositionInLine + ". ";
        final String errorMessage;
        if (Loggings.isSensitiveDataLoggingEnabled()) {
            errorMessage = location + "Please open a GitHub issue at https://github.com/debezium/dbz/issues with the statement '"
                    + parsedDdl + "'\n" + msg;
        }
        else {
            // Both the statement and the ANTLR message (which quotes the offending input) may contain sensitive data
            errorMessage = location + "The statement is not logged because it may contain sensitive data. "
                    + "Please enable TRACE logging for " + Loggings.class.getName()
                    + " and restart the connector to obtain the DDL that couldn't be parsed, then open a GitHub issue at "
                    + "https://github.com/debezium/dbz/issues with the statement.";
        }
        accumulateError.apply(new ParsingException(new Position(0, line, charPositionInLine), errorMessage, e), errors);
    }

    public Collection<ParsingException> getErrors() {
        return errors;
    }
}
