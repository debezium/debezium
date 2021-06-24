/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.List;

import io.debezium.text.ParsingException;
import io.debezium.text.TokenStream;
import io.debezium.text.TokenStream.CharacterStream;
import io.debezium.text.TokenStream.Tokenizer;
import io.debezium.text.TokenStream.Tokens;

/**
 * Parses identifiers into the corresponding parts of a {@link TableId}.
 *
 * @author Gunnar Morling.
 */
class TableIdParser {

    private static final char SEPARATOR = '.';

    public static List<String> parse(String identifier) {
        TokenStream stream = new TokenStream(identifier, new TableIdTokenizer(identifier), true);
        stream.start();

        List<String> parts = new ArrayList<>();

        while (stream.hasNext()) {
            parts.add(stream.consume().replaceAll("''", "'").replaceAll("\"\"", "\"").replaceAll("``", "`"));
        }

        return parts;
    }

    private static class TableIdTokenizer implements Tokenizer {

        private final String identifier;

        public TableIdTokenizer(String identifier) {
            this.identifier = identifier;
        }

        @Override
        public void tokenize(CharacterStream input, Tokens tokens) throws ParsingException {
            ParsingState previousState = null;
            ParsingState currentState = ParsingState.INITIAL;
            ParsingContext parsingContext = new ParsingContext(input, tokens);

            currentState.onEntry(parsingContext);

            while (input.hasNext()) {
                previousState = currentState;
                currentState = currentState.handleCharacter(input.next(), parsingContext);

                if (currentState != previousState) {
                    previousState.onExit(parsingContext);
                    currentState.onEntry(parsingContext);
                }
            }

            currentState.onExit(parsingContext);

            if (currentState != ParsingState.BEFORE_SEPARATOR && currentState != ParsingState.IN_IDENTIFIER) {
                throw new IllegalArgumentException("Invalid identifier: " + identifier);
            }
        }
    }

    private enum ParsingState {

        INITIAL {

            @Override
            ParsingState handleCharacter(char c, ParsingContext context) {
                if (Character.isWhitespace(c)) {
                    return INITIAL;
                }
                else if (c == TableIdParser.SEPARATOR) {
                    throw new IllegalArgumentException("Unexpected input: " + c);
                }
                else if (isQuotingChar(c)) {
                    context.quotingChar = c;
                    return IN_QUOTED_IDENTIFIER;
                }
                else {
                    return IN_IDENTIFIER;
                }
            }
        },

        IN_IDENTIFIER {

            @Override
            void doOnEntry(ParsingContext context) {
                context.startOfLastToken = context.input.index();
                context.lastIdentifierEnd = context.input.index();
            }

            @Override
            void doOnExit(ParsingContext context) {
                context.tokens.addToken(
                        context.input.position(context.startOfLastToken),
                        context.startOfLastToken,
                        context.lastIdentifierEnd + 1);
            }

            @Override
            ParsingState handleCharacter(char c, ParsingContext context) {
                if (Character.isWhitespace(c)) {
                    return BEFORE_SEPARATOR;
                }
                else if (c == TableIdParser.SEPARATOR) {
                    return AFTER_SEPARATOR;
                }
                else {
                    context.lastIdentifierEnd = context.input.index();
                    return IN_IDENTIFIER;
                }
            }
        },

        BEFORE_SEPARATOR {

            @Override
            ParsingState handleCharacter(char c, ParsingContext context) {
                if (Character.isWhitespace(c)) {
                    return BEFORE_SEPARATOR;
                }
                else if (c == TableIdParser.SEPARATOR) {
                    return AFTER_SEPARATOR;
                }
                else {
                    throw new IllegalArgumentException("Unexpected input: " + c);
                }
            }
        },

        AFTER_SEPARATOR {

            @Override
            ParsingState handleCharacter(char c, ParsingContext context) {
                if (Character.isWhitespace(c)) {
                    return AFTER_SEPARATOR;
                }
                else if (c == TableIdParser.SEPARATOR) {
                    throw new IllegalArgumentException("Unexpected input: " + c);
                }
                else if (isQuotingChar(c)) {
                    context.quotingChar = c;
                    return IN_QUOTED_IDENTIFIER;
                }
                else {
                    return IN_IDENTIFIER;
                }
            }
        },

        IN_QUOTED_IDENTIFIER {

            @Override
            ParsingState handleCharacter(char c, ParsingContext context) {
                if (c == context.quotingChar) {
                    if (context.escaped) {
                        context.escaped = false;
                        return IN_QUOTED_IDENTIFIER;
                    }
                    else if (context.input.isNext(context.quotingChar)) {
                        context.escaped = true;
                        return IN_QUOTED_IDENTIFIER;
                    }
                    else {
                        context.lastIdentifierEnd = context.input.index();
                        return BEFORE_SEPARATOR;
                    }
                }

                return IN_QUOTED_IDENTIFIER;
            }

            @Override
            void doOnEntry(ParsingContext context) {
                context.startOfLastToken = context.input.index();
            }

            @Override
            void doOnExit(ParsingContext context) {
                context.quotingChar = 0;
                context.tokens.addToken(
                        context.input.position(context.startOfLastToken + 1),
                        context.startOfLastToken + 1,
                        context.lastIdentifierEnd);
            }
        };

        abstract ParsingState handleCharacter(char c, ParsingContext context);

        void onEntry(ParsingContext context) {
            doOnEntry(context);
        }

        void doOnEntry(ParsingContext context) {
        }

        void onExit(ParsingContext context) {
            doOnExit(context);
        }

        void doOnExit(ParsingContext context) {
        }

        private static boolean isQuotingChar(char c) {
            return c == '"' || c == '\'' || c == '`';
        }
    }

    private static class ParsingContext {
        final CharacterStream input;
        final Tokens tokens;

        int startOfLastToken;
        int lastIdentifierEnd;
        boolean escaped;
        char quotingChar;

        public ParsingContext(CharacterStream input, Tokens tokens) {
            this.input = input;
            this.tokens = tokens;
        }
    }
}
