/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.antlr;

import io.debezium.text.ParsingException;
import io.debezium.text.Position;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.BiFunction;

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

    public ParsingErrorListener(BiFunction<ParsingException, Collection<ParsingException>, Collection<ParsingException>> accumulateError) {
        this.accumulateError = accumulateError;
    }

    /**
     * Parsing error listener, which will throw up an exception.
     */
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
        accumulateError.apply(new ParsingException(new Position(0, line, charPositionInLine), msg, e), errors);
    }

    public Collection<ParsingException> getErrors() {
        return errors;
    }
}
