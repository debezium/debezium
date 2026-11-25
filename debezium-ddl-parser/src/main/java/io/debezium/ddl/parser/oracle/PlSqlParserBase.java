/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ddl.parser.oracle;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;

import io.debezium.ddl.parser.oracle.generated.PlSqlParser;

public abstract class PlSqlParserBase extends Parser {
    private boolean _isVersion12 = true;
    private boolean _isVersion11 = true;
    private boolean _isVersion10 = true;

    /** True if the last script_unit was PL/SQL (bare '/' requires preceding ';'). */
    private boolean _lastUnitWasPlsql = false;

    public PlSqlParserBase(TokenStream input) {
        super(input);
    }

    @Override
    public void reset() {
        _lastUnitWasPlsql = false;
        super.reset();
    }

    public void setLastUnitPlsql() {
        _lastUnitWasPlsql = true;
    }

    public void setLastUnitSql() {
        _lastUnitWasPlsql = false;
    }

    public boolean isLastUnitSql() {
        return !_lastUnitWasPlsql;
    }

    public boolean isLastUnitPlsql() {
        return _lastUnitWasPlsql;
    }

    /**
     * Parser-level predicate: distinguishes SOLIDUS as a SQL*Plus separator
     * (on its own line) from SOLIDUS as a division operator (inside an expression).
     */
    public boolean isSolidusSeparator() {
        Token solidus = _input.LT(1);
        if (solidus == null || solidus.getType() != PlSqlParser.SOLIDUS) {
            return false;
        }

        int solidusLine = solidus.getLine();

        // Look-behind: previous significant token must be on a different line.
        Token prev = _input.LT(-1);
        if (prev != null && prev.getType() != Token.EOF && prev.getLine() == solidusLine) {
            return false;
        }

        // Look-ahead: next significant token must be on a different line or EOF.
        Token next = _input.LT(2);
        if (next != null && next.getType() != Token.EOF && next.getLine() == solidusLine) {
            return false;
        }

        return true;
    }

    /** True when the current token is the first significant token on its line. */
    public boolean isLineStart() {
        Token token = _input.LT(1);
        if (token == null) {
            return false;
        }

        Token prev = _input.LT(-1);
        if (prev == null || prev.getType() == Token.EOF) {
            return true;
        }

        return prev.getLine() < token.getLine();
    }

    public boolean isVersion12() {
        return _isVersion12;
    }

    public void setVersion12(boolean value) {
        _isVersion12 = value;
    }

    public boolean isVersion11() {
        return _isVersion11;
    }

    public void setVersion11(boolean value) {
        _isVersion11 = value;
    }

    public boolean isVersion10() {
        return _isVersion10;
    }

    public void setVersion10(boolean value) {
        _isVersion10 = value;
    }

    public boolean IsNotNumericFunction() {
        Token lt1 = _input.LT(1);
        Token lt2 = _input.LT(2);
        if ((lt1.getType() == PlSqlParser.SUM ||
                lt1.getType() == PlSqlParser.COUNT ||
                lt1.getType() == PlSqlParser.AVG ||
                lt1.getType() == PlSqlParser.MIN ||
                lt1.getType() == PlSqlParser.MAX ||
                lt1.getType() == PlSqlParser.ROUND ||
                lt1.getType() == PlSqlParser.LEAST ||
                lt1.getType() == PlSqlParser.GREATEST) &&
                lt2.getType() == PlSqlParser.LEFT_PAREN) {
            return false;
        }
        return true;
    }

    public boolean isNotStartOfJoin() {
        Token lt1 = _input.LT(1);
        if (lt1.getType() == PlSqlParser.JOIN ||
                lt1.getType() == PlSqlParser.INNER ||
                lt1.getType() == PlSqlParser.CROSS ||
                lt1.getType() == PlSqlParser.NATURAL ||
                lt1.getType() == PlSqlParser.PARTITION ||
                lt1.getType() == PlSqlParser.FULL ||
                lt1.getType() == PlSqlParser.LEFT ||
                lt1.getType() == PlSqlParser.RIGHT ||
                lt1.getType() == PlSqlParser.OUTER) {
            return false;
        }
        return true;
    }

    public boolean isNotBulkCollect() {
        Token lt1 = _input.LT(1);
        Token lt2 = _input.LT(2);
        return lt1.getType() != PlSqlParser.BULK || lt2.getType() != PlSqlParser.COLLECT;
    }
}
