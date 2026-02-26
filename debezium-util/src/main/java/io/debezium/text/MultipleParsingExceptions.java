/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.text;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

/**
 * Representation of multiple {@link ParsingException}s.
 * @author Randall Hauch
 */
public class MultipleParsingExceptions extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private final Collection<ParsingException> errors;

    public MultipleParsingExceptions(Collection<ParsingException> errors) {
        this("Multiple parsing errors", errors);
    }

    public MultipleParsingExceptions(String message, Collection<ParsingException> errors) {
        super(message);
        this.errors = Collections.unmodifiableCollection(errors);
    }

    /**
     * Get the set of parsing exceptions.
     * @return the parsing exceptions
     */
    public Collection<ParsingException> getErrors() {
        return errors;
    }

    public void forEachError(Consumer<ParsingException> action) {
        errors.forEach(action);
    }

    @Override
    public void printStackTrace() {
        forEachError(ParsingException::printStackTrace);
    }

    @Override
    public void printStackTrace(PrintStream s) {
        forEachError(e -> e.printStackTrace(s));
    }

    @Override
    public void printStackTrace(PrintWriter s) {
        forEachError(e -> e.printStackTrace(s));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getMessage());
        forEachError(e -> {
            sb.append(System.lineSeparator()).append(e.toString());
        });
        return sb.toString();
    }
}
