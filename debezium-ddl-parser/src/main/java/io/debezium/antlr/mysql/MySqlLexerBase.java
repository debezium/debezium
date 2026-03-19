/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

// Adapted for Debezium from https://github.com/antlr/grammars-v4/tree/master/sql/mysql/Oracle/Java
package io.debezium.antlr.mysql;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Token;

import io.debezium.ddl.parser.mysql.generated.MySqlLexer;

/** The base lexer class provides a number of functions needed in actions in the lexer (grammar). */
public abstract class MySqlLexerBase extends Lexer {

    public MySqlLexerBase(CharStream input) {
        super(input);
        this.serverVersion = 80200;
        this.sqlModes = SqlModes.sqlModeFromString("ANSI_QUOTES");
        this.charSets.addAll(List.of(
                "_armscii8", "_ascii", "_big5", "_binary", "_cp1250", "_cp1251",
                "_cp1252", "_cp1256", "_cp1257", "_cp850", "_cp852", "_cp866",
                "_cp932", "_dec8", "_eucjpms", "_euckr", "_gb18030", "_gb2312",
                "_gbk", "_geostd8", "_greek", "_hebrew", "_hp8", "_keybcs2",
                "_koi8r", "_koi8u", "_latin1", "_latin2", "_latin5", "_latin7",
                "_macce", "_macroman", "_sjis", "_swe7", "_tis620", "_ucs2",
                "_ujis", "_utf16", "_utf16le", "_utf32", "_utf8", "_utf8mb3",
                "_utf8mb4"));
    }

    public int serverVersion = 0;
    public Set<SqlMode> sqlModes = new HashSet<>();

    /** Enable Multi Language Extension support. */
    public boolean supportMle = true;

    public Set<String> charSets = new HashSet<>(); // Used to check repertoires.
    protected boolean inVersionComment = false;

    private Queue<Token> pendingTokens = new LinkedList<>();

    private static final String LONG_STRING = "2147483647";
    private static final int LONG_LENGTH = 10;
    private static final String SIGNED_LONG_STRING = "-2147483648";
    private static final String LONG_LONG_STRING = "9223372036854775807";
    private static final int LONG_LONG_LENGTH = 19;
    private static final String SIGNED_LONG_LONG_STRING = "-9223372036854775808";
    private static final int SIGNED_LONG_LONG_LENGTH = 19;
    private static final String UNSIGNED_LONG_LONG_STRING = "18446744073709551615";
    private static final int UNSIGNED_LONG_LONG_LENGTH = 20;

    private boolean justEmittedDot = false;

    public boolean isSqlModeActive(SqlMode mode) {
        return this.sqlModes.contains(mode);
    }

    @Override
    public void reset() {
        this.inVersionComment = false;
        super.reset();
    }

    @Override
    public Token nextToken() {
        Token pending = pendingTokens.poll();
        if (pending != null) {
            return pending;
        }

        Token next = super.nextToken();

        pending = pendingTokens.poll();
        if (pending != null) {
            pendingTokens.add(next);
            return pending;
        }

        return next;
    }

    protected boolean checkMySQLVersion(String text) {
        if (text.length() < 8) { // Minimum is: /*!12345
            return false;
        }

        int version = Integer.parseInt(text.substring(3));
        if (version <= this.serverVersion) {
            this.inVersionComment = true;
            return true;
        }

        return false;
    }

    protected int determineFunction(int proposed) {
        char input = (char) this._input.LA(1);
        if (this.isSqlModeActive(SqlMode.IgnoreSpace)) {
            while (input == ' ' || input == '\t' || input == '\r' || input == '\n') {
                this.getInterpreter().consume(this._input);
                this._channel = HIDDEN;
                this._type = MySqlLexer.WHITESPACE;
                input = (char) this._input.LA(1);
            }
        }
        return input == '(' ? proposed : MySqlLexer.IDENTIFIER;
    }

    protected int determineNumericType(String text) {
        int length = text.length() - 1;
        if (length < LONG_LENGTH) {
            return MySqlLexer.INT_NUMBER;
        }

        boolean negative = false;
        int index = 0;
        if (text.charAt(index) == '+') {
            ++index;
            --length;
        }
        else if (text.charAt(index) == '-') {
            ++index;
            --length;
            negative = true;
        }

        while (text.charAt(index) == '0' && length > 0) {
            ++index;
            --length;
        }

        if (length < LONG_LENGTH) {
            return MySqlLexer.INT_NUMBER;
        }

        String cmp;
        int smaller;
        int bigger;
        if (negative) {
            if (length == LONG_LENGTH) {
                cmp = SIGNED_LONG_STRING.substring(1);
                smaller = MySqlLexer.INT_NUMBER;
                bigger = MySqlLexer.LONG_NUMBER;
            }
            else if (length < SIGNED_LONG_LONG_LENGTH) {
                return MySqlLexer.LONG_NUMBER;
            }
            else if (length > SIGNED_LONG_LONG_LENGTH) {
                return MySqlLexer.DECIMAL_NUMBER;
            }
            else {
                cmp = SIGNED_LONG_LONG_STRING.substring(1);
                smaller = MySqlLexer.LONG_NUMBER;
                bigger = MySqlLexer.DECIMAL_NUMBER;
            }
        }
        else {
            if (length == LONG_LENGTH) {
                cmp = LONG_STRING;
                smaller = MySqlLexer.INT_NUMBER;
                bigger = MySqlLexer.LONG_NUMBER;
            }
            else if (length < LONG_LONG_LENGTH) {
                return MySqlLexer.LONG_NUMBER;
            }
            else if (length > LONG_LONG_LENGTH) {
                if (length > UNSIGNED_LONG_LONG_LENGTH) {
                    return MySqlLexer.DECIMAL_NUMBER;
                }
                cmp = UNSIGNED_LONG_LONG_STRING;
                smaller = MySqlLexer.ULONGLONG_NUMBER;
                bigger = MySqlLexer.DECIMAL_NUMBER;
            }
            else {
                cmp = LONG_LONG_STRING;
                smaller = MySqlLexer.LONG_NUMBER;
                bigger = MySqlLexer.ULONGLONG_NUMBER;
            }
        }

        int otherIndex = 0;
        while (index < text.length() && cmp.charAt(otherIndex++) == text.charAt(index++)) {
        }

        return text.charAt(index - 1) <= cmp.charAt(otherIndex - 1) ? smaller : bigger;
    }

    protected int checkCharset(String text) {
        return this.charSets.contains(text.toLowerCase()) ? MySqlLexer.UNDERSCORE_CHARSET : MySqlLexer.IDENTIFIER;
    }

    protected void emitDot() {
        var len = this.getText().length();
        pendingTokens.add(this._factory.create(this._tokenFactorySourcePair, MySqlLexer.DOT_SYMBOL,
                ".", this._channel, this._tokenStartCharIndex, this._tokenStartCharIndex, this.getLine(), this.getCharPositionInLine() - len));
        ++this._tokenStartCharPositionInLine;
        this.justEmittedDot = true;
    }

    @Override
    public Token emit() {
        var t = super.emit();
        if (this.justEmittedDot) {
            var p = (CommonToken) t;
            p.setText(p.getText().substring(1));
            p.setStartIndex(p.getStartIndex() + 1);
            this.justEmittedDot = false;
        }
        return t;
    }

    public boolean isMasterCompressionAlgorithm() {
        return serverVersion >= 80018 && isServerVersionLt80024();
    }

    public boolean isServerVersionGe80011() {
        return serverVersion >= 80011;
    }

    public boolean isServerVersionGe80013() {
        return serverVersion >= 80013;
    }

    public boolean isServerVersionLt80014() {
        return serverVersion < 80014;
    }

    public boolean isServerVersionGe80014() {
        return serverVersion >= 80014;
    }

    public boolean isServerVersionGe80016() {
        return serverVersion >= 80016;
    }

    public boolean isServerVersionGe80017() {
        return serverVersion >= 80017;
    }

    public boolean isServerVersionGe80018() {
        return serverVersion >= 80018;
    }

    public boolean isServerVersionLt80021() {
        return serverVersion < 80021;
    }

    public boolean isServerVersionGe80021() {
        return serverVersion >= 80021;
    }

    public boolean isServerVersionLt80022() {
        return serverVersion < 80022;
    }

    public boolean isServerVersionGe80022() {
        return serverVersion >= 80022;
    }

    public boolean isServerVersionLt80023() {
        return serverVersion < 80023;
    }

    public boolean isServerVersionGe80023() {
        return serverVersion >= 80023;
    }

    public boolean isServerVersionLt80024() {
        return serverVersion < 80024;
    }

    public boolean isServerVersionGe80024() {
        return serverVersion >= 80024;
    }

    public boolean isServerVersionLt80031() {
        return serverVersion < 80031;
    }

    public void doLogicalOr() {
        this._type = isSqlModeActive(SqlMode.PipesAsConcat) ? MySqlLexer.CONCAT_PIPES_SYMBOL : MySqlLexer.LOGICAL_OR_OPERATOR;
    }

    public void doIntNumber() {
        this._type = determineNumericType(this.getText());
    }

    public void doAdddate() {
        this._type = determineFunction(MySqlLexer.ADDDATE_SYMBOL);
    }

    public void doBitAnd() {
        this._type = determineFunction(MySqlLexer.BIT_AND_SYMBOL);
    }

    public void doBitOr() {
        this._type = determineFunction(MySqlLexer.BIT_OR_SYMBOL);
    }

    public void doBitXor() {
        this._type = determineFunction(MySqlLexer.BIT_XOR_SYMBOL);
    }

    public void doCast() {
        this._type = determineFunction(MySqlLexer.CAST_SYMBOL);
    }

    public void doCount() {
        this._type = determineFunction(MySqlLexer.COUNT_SYMBOL);
    }

    public void doCurdate() {
        this._type = determineFunction(MySqlLexer.CURDATE_SYMBOL);
    }

    public void doCurrentDate() {
        this._type = determineFunction(MySqlLexer.CURDATE_SYMBOL);
    }

    public void doCurrentTime() {
        this._type = determineFunction(MySqlLexer.CURTIME_SYMBOL);
    }

    public void doCurtime() {
        this._type = determineFunction(MySqlLexer.CURTIME_SYMBOL);
    }

    public void doDateAdd() {
        this._type = determineFunction(MySqlLexer.DATE_ADD_SYMBOL);
    }

    public void doDateSub() {
        this._type = determineFunction(MySqlLexer.DATE_SUB_SYMBOL);
    }

    public void doExtract() {
        this._type = determineFunction(MySqlLexer.EXTRACT_SYMBOL);
    }

    public void doGroupConcat() {
        this._type = determineFunction(MySqlLexer.GROUP_CONCAT_SYMBOL);
    }

    public void doMax() {
        this._type = determineFunction(MySqlLexer.MAX_SYMBOL);
    }

    public void doMid() {
        this._type = determineFunction(MySqlLexer.SUBSTRING_SYMBOL);
    }

    public void doMin() {
        this._type = determineFunction(MySqlLexer.MIN_SYMBOL);
    }

    public void doNot() {
        this._type = isSqlModeActive(SqlMode.HighNotPrecedence) ? MySqlLexer.NOT2_SYMBOL : MySqlLexer.NOT_SYMBOL;
    }

    public void doNow() {
        this._type = determineFunction(MySqlLexer.NOW_SYMBOL);
    }

    public void doPosition() {
        this._type = determineFunction(MySqlLexer.POSITION_SYMBOL);
    }

    public void doSessionUser() {
        this._type = determineFunction(MySqlLexer.USER_SYMBOL);
    }

    public void doStddevSamp() {
        this._type = determineFunction(MySqlLexer.STDDEV_SAMP_SYMBOL);
    }

    public void doStddev() {
        this._type = determineFunction(MySqlLexer.STD_SYMBOL);
    }

    public void doStddevPop() {
        this._type = determineFunction(MySqlLexer.STD_SYMBOL);
    }

    public void doStd() {
        this._type = determineFunction(MySqlLexer.STD_SYMBOL);
    }

    public void doSubdate() {
        this._type = determineFunction(MySqlLexer.SUBDATE_SYMBOL);
    }

    public void doSubstr() {
        this._type = determineFunction(MySqlLexer.SUBSTRING_SYMBOL);
    }

    public void doSubstring() {
        this._type = determineFunction(MySqlLexer.SUBSTRING_SYMBOL);
    }

    public void doSum() {
        this._type = determineFunction(MySqlLexer.SUM_SYMBOL);
    }

    public void doSysdate() {
        this._type = determineFunction(MySqlLexer.SYSDATE_SYMBOL);
    }

    public void doSystemUser() {
        this._type = determineFunction(MySqlLexer.USER_SYMBOL);
    }

    public void doTrim() {
        this._type = determineFunction(MySqlLexer.TRIM_SYMBOL);
    }

    public void doVariance() {
        this._type = determineFunction(MySqlLexer.VARIANCE_SYMBOL);
    }

    public void doVarPop() {
        this._type = determineFunction(MySqlLexer.VARIANCE_SYMBOL);
    }

    public void doVarSamp() {
        this._type = determineFunction(MySqlLexer.VAR_SAMP_SYMBOL);
    }

    public void doUnderscoreCharset() {
        this._type = checkCharset(this.getText());
    }

    public boolean doDollarQuotedStringText() {
        return this.serverVersion >= 80034 && this.supportMle;
    }

    public boolean isVersionComment() {
        return checkMySQLVersion(this.getText());
    }

    public boolean isBackTickQuotedId() {
        return !this.isSqlModeActive(SqlMode.NoBackslashEscapes);
    }

    public boolean isDoubleQuotedText() {
        return !this.isSqlModeActive(SqlMode.NoBackslashEscapes);
    }

    public boolean isSingleQuotedText() {
        return !this.isSqlModeActive(SqlMode.NoBackslashEscapes);
    }

    public void startInVersionComment() {
        inVersionComment = true;
    }

    public void endInVersionComment() {
        inVersionComment = false;
    }

    public boolean isInVersionComment() {
        return inVersionComment;
    }
}
