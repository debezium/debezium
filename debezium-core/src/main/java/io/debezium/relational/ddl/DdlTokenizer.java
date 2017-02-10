/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import io.debezium.text.ParsingException;
import io.debezium.text.Position;
import io.debezium.text.TokenStream.CharacterStream;
import io.debezium.text.TokenStream.Token;
import io.debezium.text.TokenStream.Tokenizer;
import io.debezium.text.TokenStream.Tokens;

/**
 * A {@link Tokenizer} that is specialized for tokenizing DDL streams.
 * 
 * @author Randall Hauch
 * @author Horia Chiorean
 * @author Barry LaFond
 * @author Jure Kajzer
 */
public class DdlTokenizer implements Tokenizer {

    /**
     * The {@link Token#type() token type} for tokens that represent an unquoted string
     * containing a character sequence made up of non-whitespace and non-symbol characters.
     */
    public static final int WORD = 1;
    /**
     * The {@link Token#type() token type} for tokens that consist of an individual
     * "symbol" character. The set of characters includes: <code>-(){}*,;+%?$[]!<>|=:</code>
     */
    public static final int SYMBOL = 2;
    /**
     * The {@link Token#type() token type} for tokens that consist of an individual '.'
     * character.
     */
    public static final int DECIMAL = 4;
    /**
     * The {@link Token#type() token type} for tokens that consist of all the characters
     * within single-quotes. Single quote characters are included if they are preceded (escaped) by a '\' character.
     */
    public static final int SINGLE_QUOTED_STRING = 8;
    /**
     * The {@link Token#type() token type} for tokens that consist of all the characters
     * within double-quotes. Double quote characters are included if they are preceded (escaped) by a '\' character.
     */
    public static final int DOUBLE_QUOTED_STRING = 16;
    /**
     * The {@link Token#type() token type} for tokens that consist of all the characters
     * between "/*" and "&#42;/", between "//" and the next line terminator (e.g., '\n', '\r' or "\r\n"), between "#" and
     * the next line terminator (e.g., '\n', '\r' or "\r\n"), or between "--" and
     * the next line terminator (e.g., '\n', '\r' or "\r\n").
     */
    public static final int COMMENT = 32;

    /**
     * The {@link Token#type() token type} for tokens that represent key words or
     * reserved words for a given DDL dialect.
     * <p>
     * Examples would be: "CREATE", "TABLE", "ALTER", "SCHEMA", "DROP", etc...
     * </p>
     */
    public static final int KEYWORD = 64;

    /**
     * The {@link Token#type() token type} for tokens that represent the start of a DDL
     * statement.
     * <p>
     * Examples would be: {"CREATE", "TABLE"} {"CREATE", "OR", "REPLACE", "VIEW"}
     * </p>
     */
    public static final int STATEMENT_KEY = 128;

    /**
     * The {@link Token#type() token type} for tokens that represent the end of a DDL statement.
     */
    public static final int STATEMENT_TERMINATOR = 256;

    public static interface TokenTypeFunction {
        /**
         * Determine the type of the token.
         * 
         * @param type the type of the token
         * @param token the token
         * @return the potentially modified token type
         */
        int typeOf(int type, String token);
    }

    private final boolean removeQuotes = true;
    private final boolean useComments;
    private final TokenTypeFunction retypingFunction;

    public DdlTokenizer(boolean useComments) {
        this(useComments, null);
    }

    public DdlTokenizer(boolean useComments, TokenTypeFunction retypingFunction) {
        this.useComments = useComments;
        this.retypingFunction = retypingFunction != null ? retypingFunction : (type, token) -> type;
    }

    /**
     * @return useComments
     */
    public boolean includeComments() {
        return useComments;
    }

    protected Tokens adapt(CharacterStream input,
                           Tokens output) {
        return (position, startIndex, endIndex, type) -> {
            output.addToken(position, startIndex, endIndex,
                            retypingFunction.typeOf(type, input.substring(startIndex, endIndex).toUpperCase()));
        };
    }

    @Override
    public void tokenize(CharacterStream input,
                         Tokens tokens)
            throws ParsingException {
        tokens = adapt(input, tokens);
        int startIndex;
        int endIndex;
        while (input.hasNext()) {
            char c = input.next();
            switch (c) {
                case ' ':
                case '\t':
                case '\n':
                case '\r':
                    // Just skip these whitespace characters ...
                    break;

                // ==============================================================================================
                // DDL Comments token = "#"
                // ==============================================================================================
                case '#': {
                    startIndex = input.index();
                    Position startPosition = input.position(startIndex);
                    // End-of-line comment ...
                    boolean foundLineTerminator = false;
                    while (input.hasNext()) {
                        c = input.next();
                        if (c == '\n' || c == '\r') {
                            foundLineTerminator = true;
                            break;
                        }
                    }
                    endIndex = input.index(); // the token won't include the '\n' or '\r' character(s)
                    if (!foundLineTerminator) ++endIndex; // must point beyond last char
                    if (c == '\r' && input.isNext('\n')) input.next();
                    if (useComments) {
                        tokens.addToken(startPosition, startIndex, endIndex, COMMENT);
                    }
                    break;
                }
                // ==============================================================================================
                // DDL Comments token = "--"
                // ==============================================================================================
                case '-': {
                    startIndex = input.index();
                    Position startPosition = input.position(startIndex);
                    if (input.isNext('-')) {
                        // -- END OF LINE comment ...
                        boolean foundLineTerminator = false;
                        while (input.hasNext()) {
                            c = input.next();
                            if (c == '\n' || c == '\r') {
                                foundLineTerminator = true;
                                break;
                            }
                        }
                        endIndex = input.index(); // the token won't include the '\n' or '\r' character(s)
                        if (!foundLineTerminator) ++endIndex; // must point beyond last char
                        if (c == '\r' && input.isNext('\n')) input.next();

                        // Check for PARSER_ID

                        if (useComments) {
                            tokens.addToken(startPosition, startIndex, endIndex, COMMENT);
                        }

                    } else {
                        // just a regular dash ...
                        tokens.addToken(startPosition, startIndex, startIndex + 1, SYMBOL);
                    }
                    break;
                }
                // ==============================================================================================
                case '(':
                case ')':
                case '{':
                case '}':
                case '*':
                case ',':
                case ';':
                case '+':
                case '%':
                case '?':
                case '[':
                case ']':
                case '!':
                case '<':
                case '>':
                case '|':
                case '=':
                case ':':
                    tokens.addToken(input.position(input.index()), input.index(), input.index() + 1, SYMBOL);
                    break;
                case '.':
                    tokens.addToken(input.position(input.index()), input.index(), input.index() + 1, DECIMAL);
                    break;
                case '\"':
                    startIndex = input.index();
                    Position startingPosition = input.position(startIndex);
                    boolean foundClosingQuote = false;
                    while (input.hasNext()) {
                        c = input.next();
                        if ((c == '\\' || c == '"') && input.isNext('"')) {
                            c = input.next(); // consume the ' character since it is escaped
                        } else if (c == '"') {
                            foundClosingQuote = true;
                            break;
                        }
                    }
                    if (!foundClosingQuote) {
                        String msg = "No matching double quote found after at line " + startingPosition.line() + ", column "
                                + startingPosition.column();
                        throw new ParsingException(startingPosition, msg);
                    }
                    endIndex = input.index() + 1; // beyond last character read
                    if ( removeQuotes && endIndex - startIndex > 1 ) {
                        // At least one quoted character, so remove the quotes ...
                        startIndex += 1;
                        endIndex -= 1;
                    }
                    tokens.addToken(startingPosition, startIndex, endIndex, DOUBLE_QUOTED_STRING);
                    break;
                case '`':       // back-quote character
                case '\u2018': // left single-quote character
                case '\u2019': // right single-quote character
                case '\'':     // single-quote character
                    char quoteChar = c;
                    startIndex = input.index();
                    startingPosition = input.position(startIndex);
                    foundClosingQuote = false;
                    while (input.hasNext()) {
                        c = input.next();
                        if ((c == '\\' || c == quoteChar) && input.isNext(quoteChar)) {
                            c = input.next(); // consume the character since it is escaped
                        } else if (c == quoteChar) {
                            foundClosingQuote = true;
                            break;
                        }
                    }
                    if (!foundClosingQuote) {
                        String msg = "No matching single quote found after line " + startingPosition.line() + ", column "
                                + startingPosition.column();
                        throw new ParsingException(startingPosition, msg);
                    }
                    endIndex = input.index() + 1; // beyond last character read
                    if ( removeQuotes && endIndex - startIndex > 1 ) {
                        // At least one quoted character, so remove the quotes ...
                        startIndex += 1;
                        endIndex -= 1;
                    }
                    tokens.addToken(startingPosition, startIndex, endIndex, SINGLE_QUOTED_STRING);
                    break;
                case '/':
                    startIndex = input.index();
                    startingPosition = input.position(startIndex);
                    if (input.isNext('/')) {
                        // End-of-line comment ...
                        boolean foundLineTerminator = false;
                        while (input.hasNext()) {
                            c = input.next();
                            if (c == '\n' || c == '\r') {
                                foundLineTerminator = true;
                                break;
                            }
                        }
                        endIndex = input.index(); // the token won't include the '\n' or '\r' character(s)
                        if (!foundLineTerminator) ++endIndex; // must point beyond last char
                        if (c == '\r' && input.isNext('\n')) input.next();
                        if (useComments) {
                            tokens.addToken(startingPosition, startIndex, endIndex, COMMENT);
                        }

                    } else if (input.isNext('*')) {
                        // Multi-line comment ...
                        while (input.hasNext() && !input.isNext('*', '/')) {
                            c = input.next();
                        }
                        if (input.hasNext()) input.next(); // consume the '*'
                        if (input.hasNext()) input.next(); // consume the '/'

                        endIndex = input.index() + 1; // the token will include the '/' and '*' characters
                        if (useComments) {
                            tokens.addToken(startingPosition, startIndex, endIndex, COMMENT);
                        }

                    } else {
                        // just a regular slash ...
                        tokens.addToken(startingPosition, startIndex, startIndex + 1, SYMBOL);
                    }
                    break;
                default:
                    startIndex = input.index();
                    Position startPosition = input.position(startIndex);
                    // Read until another whitespace/symbol/decimal/slash/quote is found
                    while (input.hasNext() && !(input.isNextWhitespace() || input.isNextAnyOf("/.-(){}*,;+%?[]!<>|=:'`\u2018\u2019\"\u2019"))) {
                        c = input.next();
                    }
                    endIndex = input.index() + 1; // beyond last character that was included
                    tokens.addToken(startPosition, startIndex, endIndex, WORD);
            }
        }
    }
}
