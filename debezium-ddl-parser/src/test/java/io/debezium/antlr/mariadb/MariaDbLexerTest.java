/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.antlr.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.ddl.parser.mariadb.generated.MariaDBLexer;

class MariaDbLexerTest {

    @ParameterizedTest
    @MethodSource("stringLiterals")
    void shouldPreserveLiteralTextAndBoundaries(boolean noBackslashEscapes, String literal, int tokenType) {
        final var lexer = lexer(literal + ",suffix", noBackslashEscapes);

        final var tokens = assertTokens(lexer, literal, ",", "suffix");

        assertThat(tokens).extracting(Token::getType).containsExactly(tokenType, MariaDBLexer.COMMA, MariaDBLexer.ID);
        assertThat(tokens).extracting(Token::getChannel).containsOnly(Token.DEFAULT_CHANNEL);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldPreserveCharsetIntroducer(boolean noBackslashEscapes) {
        final var literal = noBackslashEscapes ? "'한😀\\'" : "'한😀\\\\'";
        final var lexer = lexer("_utf8mb4" + literal + ",suffix", noBackslashEscapes);

        final var tokens = assertTokens(lexer, "_utf8mb4", literal, ",", "suffix");

        assertThat(tokens).extracting(Token::getType)
                .containsExactly(MariaDBLexer.STRING_CHARSET_NAME, MariaDBLexer.STRING_LITERAL, MariaDBLexer.COMMA, MariaDBLexer.ID);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldLeaveIdentifiersCommentsAndNullUnchanged(boolean noBackslashEscapes) {
        final var identifier = "`a\\``b`";
        final var blockComment = "/* \\'\" */";
        final var hashComment = "# \\'\"\n";
        final var lineComment = "-- \\'\"\n";
        final var mysqlComment = "/*! \\'\" */";
        final var lexer = lexer(identifier + "," + blockComment + hashComment + lineComment + mysqlComment + "\\N", noBackslashEscapes);

        final var tokens = assertTokens(lexer, identifier, ",", blockComment, hashComment, lineComment, mysqlComment, "\\N");

        assertThat(tokens).extracting(Token::getType)
                .containsExactly(MariaDBLexer.STRING_LITERAL, MariaDBLexer.COMMA, MariaDBLexer.COMMENT_INPUT,
                        MariaDBLexer.LINE_COMMENT, MariaDBLexer.LINE_COMMENT, MariaDBLexer.SPEC_MYSQL_COMMENT, MariaDBLexer.NULL_SPEC_LITERAL);
        assertThat(tokens).extracting(Token::getChannel)
                .containsExactly(Token.DEFAULT_CHANNEL, Token.DEFAULT_CHANNEL, Token.HIDDEN_CHANNEL, Token.HIDDEN_CHANNEL,
                        Token.HIDDEN_CHANNEL, MariaDBLexer.MYSQLCOMMENT, Token.DEFAULT_CHANNEL);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldKeepModeIndependentBetweenLexerInstances(boolean noBackslashEscapes) {
        final var input = "'a\\'b'";
        final var first = lexer(input, noBackslashEscapes);
        final var second = lexer(input, !noBackslashEscapes);

        assertThat(first.nextToken().getText()).isEqualTo(noBackslashEscapes ? "'a\\'" : input);
        assertThat(second.nextToken().getText()).isEqualTo(noBackslashEscapes ? input : "'a\\'");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReevaluateModeWhenLexerIsReused(boolean noBackslashEscapes) {
        final var input = "'a\\'b'";
        final var lexer = lexer(input, noBackslashEscapes);

        assertThat(lexer.nextToken().getText()).isEqualTo(noBackslashEscapes ? "'a\\'" : input);
        lexer.setInputStream(CharStreams.fromString(input));
        lexer.setNoBackslashEscapes(!noBackslashEscapes);

        assertThat(lexer.nextToken().getText()).isEqualTo(noBackslashEscapes ? input : "'a\\'");
    }

    private static MariaDBLexer lexer(String input, boolean noBackslashEscapes) {
        final var lexer = new MariaDBLexer(CharStreams.fromString(input));
        lexer.setNoBackslashEscapes(noBackslashEscapes);
        return lexer;
    }

    private static List<? extends Token> assertTokens(MariaDBLexer lexer, String... expectedTexts) {
        final List<? extends Token> tokens = lexer.getAllTokens();
        assertThat(tokens).extracting(Token::getText).containsExactly(expectedTexts);
        var position = 0;
        for (final var token : tokens) {
            assertThat(token.getStartIndex()).isEqualTo(position);
            position += token.getText().codePointCount(0, token.getText().length());
            assertThat(token.getStopIndex()).isEqualTo(position - 1);
        }
        assertThat(position).isEqualTo(lexer.getInputStream().size());
        return tokens;
    }

    private static Stream<Arguments> stringLiterals() {
        return Stream.of(
                Arguments.of(false, "'a\\'b'", MariaDBLexer.STRING_LITERAL),
                Arguments.of(true, "'a\\''b'", MariaDBLexer.STRING_LITERAL),
                Arguments.of(false, "\"a\\\"b\"", MariaDBLexer.STRING_LITERAL),
                Arguments.of(true, "\"a\\\"\"b\"", MariaDBLexer.STRING_LITERAL),
                Arguments.of(false, "'a\\\\'", MariaDBLexer.STRING_LITERAL),
                Arguments.of(true, "'a\\'", MariaDBLexer.STRING_LITERAL),
                Arguments.of(false, "\"a\\\\\"", MariaDBLexer.STRING_LITERAL),
                Arguments.of(true, "\"a\\\"", MariaDBLexer.STRING_LITERAL),
                Arguments.of(false, "n'한😀\\\\'", MariaDBLexer.START_NATIONAL_STRING_LITERAL),
                Arguments.of(true, "N'한😀\\'", MariaDBLexer.START_NATIONAL_STRING_LITERAL),
                Arguments.of(false, "@'a\\\\'", MariaDBLexer.LOCAL_ID),
                Arguments.of(true, "@'a\\'", MariaDBLexer.LOCAL_ID),
                Arguments.of(false, "@\"a\\\\\"", MariaDBLexer.LOCAL_ID),
                Arguments.of(true, "@\"a\\\"", MariaDBLexer.LOCAL_ID));
    }
}
