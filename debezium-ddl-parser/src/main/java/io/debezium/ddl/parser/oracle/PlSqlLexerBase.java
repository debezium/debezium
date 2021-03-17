/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ddl.parser.oracle;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;

public abstract class PlSqlLexerBase extends Lexer {
    public PlSqlLexerBase(CharStream input) {
        super(input);
    }

    protected boolean IsNewlineAtPos(int pos) {
        int la = _input.LA(pos);
        return la == -1 || la == '\n';
    }
}
