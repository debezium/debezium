/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.antlr.mariadb;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;

/**
 * Provides SQL mode predicates for the MariaDB lexer grammar.
 */
public abstract class MariaDbLexerBase extends Lexer {

    private boolean noBackslashEscapes;

    public MariaDbLexerBase(CharStream input) {
        super(input);
    }

    public boolean isNoBackslashEscapes() {
        return noBackslashEscapes;
    }

    public void setNoBackslashEscapes(boolean noBackslashEscapes) {
        this.noBackslashEscapes = noBackslashEscapes;
    }
}
