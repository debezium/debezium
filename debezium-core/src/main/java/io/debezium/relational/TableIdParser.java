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
    private static final String SINGLE_QUOTES = "''";
    private static final String DOUBLE_QUOTES = "\"\"";
    private static final String BACKTICKS = "``";

    public static List<String> parse(String identifier) {
        return parse(identifier, new TableIdPredicates() {
        });
    }

    public static List<String> parse(String identifier, TableIdPredicates predicates) {
        TokenStream stream = new TokenStream(identifier, new TableIdTokenizer(identifier, predicates), true);
        stream.start();

        // at max three parts - catalog.schema.table
        List<String> parts = new ArrayList<>(3);

        while (stream.hasNext()) {
            parts.add(stream.consume()
                    .replace(SINGLE_QUOTES, "'")
                    .replace(DOUBLE_QUOTES, "\"")
                    .replace(BACKTICKS, "`"));
        }

        return parts;
    }

    private static class TableIdTokenizer implements Tokenizer {

        private final String identifier;
        private final TableIdPredicates predicates;

        TableIdTokenizer(String identifier, TableIdPredicates predicates) {
            this.identifier = identifier;
            this.predicates = predicates;
        }

        @Override
        public void tokenize(CharacterStream input, Tokens tokens) throws ParsingException {
            ParsingState previousState = null;
            ParsingState currentState = ParsingState.INITIAL;
            ParsingContext parsingContext = new ParsingContext(input, tokens, predicates);

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
                else if (context.predicates.isQuotingChar(c)) {
                    context.quotingChar = c;
                    return IN_QUOTED_IDENTIFIER;
                }
                else if (context.predicates.isStartDelimiter(c)) {
                    return IN_DELIMITED_IDENTIFIER;
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
                else if (context.predicates.isQuotingChar(c)) {
                    context.quotingChar = c;
                    return IN_QUOTED_IDENTIFIER;
                }
                else if (context.predicates.isStartDelimiter(c)) {
                    return IN_DELIMITED_IDENTIFIER;
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
        },

        IN_DELIMITED_IDENTIFIER {

            @Override
            ParsingState handleCharacter(char c, ParsingContext context) {
                if (context.predicates.isEndDelimiter(c)) {
                    context.lastIdentifierEnd = context.input.index();
                    return BEFORE_SEPARATOR;
                }

                return IN_DELIMITED_IDENTIFIER;
            }

            @Override
            void doOnEntry(ParsingContext context) {
                context.startOfLastToken = context.input.index();
            }

            @Override
            void doOnExit(ParsingContext context) {
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
    }

    private static class ParsingContext {
        final CharacterStream input;
        final Tokens tokens;
        final TableIdPredicates predicates;

        int startOfLastToken;
        int lastIdentifierEnd;
        boolean escaped;
        char quotingChar;

        ParsingContext(CharacterStream input, Tokens tokens, TableIdPredicates predicates) {
            this.input = input;
            this.tokens = tokens;
            this.predicates = predicates;
        }
    }
}
