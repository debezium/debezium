/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.text;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import io.debezium.text.TokenStream.BasicTokenizer;
import io.debezium.text.TokenStream.Tokenizer;

/**
 * @author Randall Hauch
 * @author Daniel Kelleher
 */
public class TokenStreamTest {
    public static final int WORD = TokenStream.BasicTokenizer.WORD;
    public static final int SYMBOL = TokenStream.BasicTokenizer.SYMBOL;
    public static final int DECIMAL = TokenStream.BasicTokenizer.DECIMAL;
    public static final int SINGLE_QUOTED_STRING = TokenStream.BasicTokenizer.SINGLE_QUOTED_STRING;
    public static final int DOUBLE_QUOTED_STRING = TokenStream.BasicTokenizer.DOUBLE_QUOTED_STRING;
    public static final int COMMENT = TokenStream.BasicTokenizer.COMMENT;

    private Tokenizer tokenizer;
    private String content;
    private TokenStream tokens;

    @Before
    public void beforeEach() {
        tokenizer = TokenStream.basicTokenizer(false);
        content = "Select all columns from this table";
        makeCaseInsensitive();
    }

    public void makeCaseSensitive() {
        tokens = new TokenStream(content, tokenizer, true);
        tokens.start();
    }

    public void makeCaseInsensitive() {
        tokens = new TokenStream(content, tokenizer, false);
        tokens.start();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotAllowConsumeBeforeStartIsCalled() {
        tokens = new TokenStream(content, TokenStream.basicTokenizer(false), false);
        tokens.consume("Select");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotAllowHasNextBeforeStartIsCalled() {
        tokens = new TokenStream(content, TokenStream.basicTokenizer(false), false);
        tokens.hasNext();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotAllowMatchesBeforeStartIsCalled() {
        tokens = new TokenStream(content, TokenStream.basicTokenizer(false), false);
        tokens.matches("Select");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotAllowCanConsumeBeforeStartIsCalled() {
        tokens = new TokenStream(content, TokenStream.basicTokenizer(false), false);
        tokens.canConsume("Select");
    }

    @Test
    public void shouldReturnTrueFromHasNextIfThereIsACurrentToken() {
        content = "word";
        makeCaseSensitive();
        assertThat(tokens.currentToken().matches("word")).isTrue();
        assertThat(tokens.hasNext()).isTrue();
    }

    @Test
    public void shouldConsumeInCaseSensitiveMannerWithExpectedValuesWhenMatchingExactCase() {
        makeCaseSensitive();
        tokens.consume("Select");
        tokens.consume("all");
        tokens.consume("columns");
        tokens.consume("from");
        tokens.consume("this");
        tokens.consume("table");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test(expected = ParsingException.class)
    public void shouldFailToConsumeInCaseSensitiveMannerWithExpectedValuesWhenMatchingIncorrectCase() {
        makeCaseSensitive();
        tokens.consume("Select");
        tokens.consume("all");
        tokens.consume("Columns");
    }

    @Test
    public void shouldConsumeInCaseInsensitiveMannerWithExpectedValuesWhenMatchingNonExactCase() {
        makeCaseInsensitive();
        tokens.consume("SELECT");
        tokens.consume("ALL");
        tokens.consume("COLUMNS");
        tokens.consume("FROM");
        tokens.consume("THIS");
        tokens.consume("TABLE");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test(expected = ParsingException.class)
    public void shouldFailToConsumeInCaseInsensitiveMannerWithExpectedValuesWhenMatchingStringIsInLowerCase() {
        makeCaseInsensitive();
        tokens.consume("SELECT");
        tokens.consume("ALL");
        tokens.consume("columns");
    }

    @Test
    public void shouldHandleNonAsciiCharactersWhenCaseSensitive() {
        content = "ü and";
        makeCaseSensitive();
        tokens.consume("ü");
        tokens.consume("and");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldHandleßCharacterWhenCaseSensitive() {
        content = "ß and";
        makeCaseSensitive();
        tokens.consume("ß");
        tokens.consume("and");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldConsumeCaseInsensitiveStringInOriginalCase() {
        makeCaseInsensitive();
        String firstToken = tokens.consume();

        assertThat(firstToken).isEqualTo("Select");
    }

    @Test
    public void shouldMatchUpperCaseVersionOfßCharacterWhenCaseInsensitive() {
        content = "ß";
        makeCaseInsensitive();
        tokens.consume("SS");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldHandleTokensAfterßCharacterWhenCaseInsensitive() {
        content = "ß and";
        makeCaseInsensitive();
        tokens.consume(TokenStream.ANY_VALUE);
        tokens.consume("AND");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueFromCanConsumeWithCaseSensitiveTokenStreamIfMatchStringDoesMatchCaseExactly() {
        makeCaseSensitive();
        assertThat(tokens.canConsume("Select")).isTrue();
        assertThat(tokens.canConsume("all")).isTrue();
        assertThat(tokens.canConsume("columns")).isTrue();
        assertThat(tokens.canConsume("from")).isTrue();
        assertThat(tokens.canConsume("this")).isTrue();
        assertThat(tokens.canConsume("table")).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnFalseFromCanConsumeWithCaseSensitiveTokenStreamIfMatchStringDoesNotMatchCaseExactly() {
        makeCaseSensitive();
        assertThat(tokens.canConsume("Select")).isTrue();
        assertThat(tokens.canConsume("all")).isTrue();
        assertThat(tokens.canConsume("Columns")).isFalse();
        assertThat(tokens.canConsume("COLUMNS")).isFalse();
        assertThat(tokens.canConsume("columns")).isTrue();
        assertThat(tokens.canConsume("from")).isTrue();
        assertThat(tokens.canConsume("THIS")).isFalse();
        assertThat(tokens.canConsume("table")).isFalse();
        assertThat(tokens.canConsume("this")).isTrue();
        assertThat(tokens.canConsume("table")).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueFromCanConsumeWithCaseSensitiveTokenStreamIfSuppliedTypeDoesMatch() {
        makeCaseSensitive();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnFalseFromCanConsumeWithCaseSensitiveTokenStreamIfSuppliedTypeDoesMatch() {
        makeCaseSensitive();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(COMMENT)).isFalse();
        assertThat(tokens.canConsume(SINGLE_QUOTED_STRING)).isFalse();
        assertThat(tokens.canConsume(DOUBLE_QUOTED_STRING)).isFalse();
        assertThat(tokens.canConsume(DECIMAL)).isFalse();
        assertThat(tokens.canConsume(SYMBOL)).isFalse();

        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueFromMatchesWithCaseSensitiveTokenStreamIfMatchStringDoesMatchCaseExactly() {
        makeCaseSensitive();
        assertThat(tokens.matches("Select")).isTrue();
        assertThat(tokens.matches("select")).isFalse();
        assertThat(tokens.canConsume("Select")).isTrue();
        assertThat(tokens.matches("all")).isTrue();
        assertThat(tokens.canConsume("all")).isTrue();
    }

    @Test
    public void shouldReturnFalseFromMatchesWithCaseSensitiveTokenStreamIfMatchStringDoesMatchCaseExactly() {
        makeCaseSensitive();
        assertThat(tokens.matches("select")).isFalse();
        assertThat(tokens.matches("SElect")).isFalse();
        assertThat(tokens.matches("Select")).isTrue();
    }

    @Test
    public void shouldReturnFalseFromCanConsumeWithCaseInsensitiveTokenStreamIfMatchStringIsNotUppercase() {
        makeCaseInsensitive();
        assertThat(tokens.canConsume("Select")).isFalse();
        assertThat(tokens.canConsume("SELECT")).isTrue();
        assertThat(tokens.canConsume("aLL")).isFalse();
        assertThat(tokens.canConsume("all")).isFalse();
        assertThat(tokens.canConsume("ALL")).isTrue();
    }

    @Test
    public void shouldReturnTrueFromCanConsumeWithCaseInsensitiveTokenStreamIfMatchStringDoesNotMatchCaseExactly() {
        makeCaseInsensitive();
        assertThat(tokens.canConsume("SELECT")).isTrue();
        assertThat(tokens.canConsume("ALL")).isTrue();
        assertThat(tokens.canConsume("COLUMNS")).isTrue();
        assertThat(tokens.canConsume("FROM")).isTrue();
        assertThat(tokens.canConsume("THIS")).isTrue();
        assertThat(tokens.canConsume("TABLE")).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueFromCanConsumeWithCaseInsensitiveTokenStreamIfSuppliedTypeDoesMatch() {
        makeCaseInsensitive();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnFalseFromCanConsumeWithCaseInsensitiveTokenStreamIfSuppliedTypeDoesMatch() {
        makeCaseInsensitive();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(COMMENT)).isFalse();
        assertThat(tokens.canConsume(SINGLE_QUOTED_STRING)).isFalse();
        assertThat(tokens.canConsume(DOUBLE_QUOTED_STRING)).isFalse();
        assertThat(tokens.canConsume(DECIMAL)).isFalse();
        assertThat(tokens.canConsume(SYMBOL)).isFalse();

        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.canConsume(WORD)).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueFromMatchesWithCaseInsensitiveTokenStreamIfMatchStringIsUppercaseAndMatches() {
        makeCaseInsensitive();
        assertThat(tokens.matches("SELECT")).isTrue();
        assertThat(tokens.canConsume("SELECT")).isTrue();
        assertThat(tokens.matches("ALL")).isTrue();
        assertThat(tokens.canConsume("ALL")).isTrue();
    }

    @Test
    public void shouldReturnFalseFromMatchesWithCaseInsensitiveTokenStreamIfMatchStringIsUppercaseAndDoesNotMatch() {
        makeCaseInsensitive();
        assertThat(tokens.matches("ALL")).isFalse();
        assertThat(tokens.matches("SElect")).isFalse();
        assertThat(tokens.matches("SELECT")).isTrue();
    }

    @Test
    public void shouldConsumeMultipleTokensIfTheyMatch() {
        makeCaseInsensitive();
        tokens.consume("SELECT", "ALL", "COLUMNS", "FROM", "THIS", "TABLE");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test(expected = ParsingException.class)
    public void shouldFailToConsumeMultipleTokensIfTheyDoNotMatch() {
        makeCaseInsensitive();
        tokens.consume("SELECT", "ALL", "COLUMNS", "FROM", "TABLE");
    }

    @Test
    public void shouldReturnTrueFromCanConsumeMultipleTokensIfTheyAllMatch() {
        makeCaseInsensitive();
        assertThat(tokens.canConsume("SELECT", "ALL", "COLUMNS", "FROM", "THIS", "TABLE")).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueFromCanConsumeArrayOfTokensIfTheyAllMatch() {
        makeCaseInsensitive();
        assertThat(tokens.matches(new String[]{ "SELECT", "ALL", "COLUMNS", "FROM", "THIS", "TABLE" })).isTrue();
        assertThat(tokens.canConsume(new String[]{ "SELECT", "ALL", "COLUMNS", "FROM", "THIS", "TABLE" })).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueFromCanConsumeMultipleTokensIfTheyDoNotAllMatch() {
        makeCaseInsensitive();
        // Unable to consume unless they all match ...
        assertThat(tokens.canConsume("SELECT", "ALL", "COLUMNS", "FRM", "THIS", "TABLE")).isFalse();
        assertThat(tokens.canConsume("SELECT", "ALL", "COLUMNS", "FROM", "THIS", "TABLE", "EXTRA")).isFalse();
        assertThat(tokens.canConsume("SELECT", "ALL", "COLUMNS", "FROM", "EXTRA", "THIS", "TABLE")).isFalse();
        assertThat(tokens.hasNext()).isTrue();
        // Should have consumed nothing so far ...
        assertThat(tokens.canConsume("SELECT", "ALL", "COLUMNS")).isTrue();
        assertThat(tokens.canConsume("FROM", "THIS", "TABLE")).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueFromMatchAnyOfIfAnyOfTheTokenValuesMatch() {
        makeCaseInsensitive();
        // Unable to consume unless they all match ...
        assertThat(tokens.matchesAnyOf("ALL", "COLUMNS")).isFalse();
        assertThat(tokens.matchesAnyOf("ALL", "COLUMNS", "SELECT")).isTrue();
        tokens.consume("SELECT");
        assertThat(tokens.matchesAnyOf("ALL", "COLUMNS", "SELECT")).isTrue();
        tokens.consume("ALL");
        assertThat(tokens.matchesAnyOf("ALL", "COLUMNS", "SELECT")).isTrue();
        tokens.consume("COLUMNS");
        assertThat(tokens.canConsume("FROM", "THIS", "TABLE")).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueFromMatchIfAllTypeValuesMatch() {
        makeCaseInsensitive();
        assertThat(tokens.matches(BasicTokenizer.WORD, BasicTokenizer.WORD)).isTrue();
    }

    @Test
    public void shouldReturnFalseFromMatchIfAllTypeValuesDoNotMatch() {
        makeCaseInsensitive();
        assertThat(tokens.matches(BasicTokenizer.WORD, BasicTokenizer.DECIMAL)).isFalse();
        assertThat(tokens.matches(BasicTokenizer.DECIMAL, BasicTokenizer.WORD)).isFalse();
    }

    @Test
    public void shouldConsumeMultipleTokensWithAnyValueConstant() {
        makeCaseInsensitive();
        // Unable to consume unless they all match ...
        tokens.consume("SELECT", "ALL", TokenStream.ANY_VALUE);
        tokens.consume("FROM", "THIS", "TABLE");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldConsumeTokenWithAnyValueConstant() {
        makeCaseInsensitive();
        // Unable to consume unless they all match ...
        tokens.consume("SELECT", "ALL");
        tokens.consume(TokenStream.ANY_VALUE);
        tokens.consume("FROM", "THIS", "TABLE");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueFromCanConsumeMultipleTokensWithAnyValueConstant() {
        makeCaseInsensitive();
        // Unable to consume unless they all match ...
        assertThat(tokens.canConsume("SELECT", "ALL", TokenStream.ANY_VALUE, "FRM", "THIS", "TABLE")).isFalse();
        assertThat(tokens.canConsume("SELECT", "ALL", "COLUMNS", "FROM", TokenStream.ANY_VALUE, "TABLE")).isTrue();
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldCanConsumeSingleAfterTokensCompleteFromCanConsumeStringList() {
        makeCaseInsensitive();
        // consume ALL the tokens using canConsume()
        tokens.canConsume("SELECT", "ALL", "COLUMNS", "FROM", "THIS", "TABLE");
        // try to canConsume() single word
        assertThat(tokens.canConsume("SELECT")).isFalse();
        assertThat(tokens.canConsume(TokenStream.ANY_VALUE)).isFalse();
        assertThat(tokens.canConsume(BasicTokenizer.SYMBOL)).isFalse();
    }

    @Test
    public void shouldCanConsumeStringAfterTokensCompleteFromCanConsumeStringArray() {
        makeCaseInsensitive();
        // consume ALL the tokens using canConsume()
        tokens.canConsume(new String[]{ "SELECT", "ALL", "COLUMNS", "FROM", "THIS", "TABLE" });
        // try to canConsume() single word
        assertThat(tokens.canConsume("SELECT")).isFalse();
        assertThat(tokens.canConsume(TokenStream.ANY_VALUE)).isFalse();
        assertThat(tokens.canConsume(BasicTokenizer.SYMBOL)).isFalse();
    }

    @Test
    public void shouldCanConsumeStringAfterTokensCompleteFromCanConsumeStringIterator() {
        makeCaseInsensitive();
        // consume ALL the tokens using canConsume()
        tokens.canConsume(Arrays.asList(new String[]{ "SELECT", "ALL", "COLUMNS", "FROM", "THIS", "TABLE" }));
        // try to canConsume() single word
        assertThat(tokens.canConsume("SELECT")).isFalse();
        assertThat(tokens.canConsume(TokenStream.ANY_VALUE)).isFalse();
        assertThat(tokens.canConsume(BasicTokenizer.SYMBOL)).isFalse();
    }

    @Test
    public void shouldFindNextPositionStartIndex() {
        makeCaseInsensitive();
        // "Select all columns from this table";
        tokens.consume();
        // Next position should be line 1, column 8
        assertThat(tokens.nextPosition().index()).isEqualTo(7);
        assertThat(tokens.nextPosition().column()).isEqualTo(8);
        assertThat(tokens.nextPosition().line()).isEqualTo(1);
    }

    @Test
    public void shouldFindPreviousPositionStartIndex() {
        makeCaseInsensitive();
        // "Select all columns from this table";
        tokens.consume();
        tokens.consume();
        // previous position should be line 1, column 8
        assertThat(tokens.previousPosition().index()).isEqualTo(7);
        assertThat(tokens.previousPosition().column()).isEqualTo(8);
        assertThat(tokens.previousPosition().line()).isEqualTo(1);
    }

    @Test
    public void shouldParseMultiLineString() {
        makeCaseInsensitive();
        String content = "ALTER DATABASE \n" + "DO SOMETHING; \n" + "ALTER DATABASE \n" + "      SET DEFAULT BIGFILE TABLESPACE;";
        tokens = new TokenStream(content, tokenizer, true);
        tokens.start();

        tokens.consume(); // LINE
        tokens.consume(); // ONE
        tokens.consume(); // DO
        tokens.consume(); // SOMETHING
        tokens.consume(); // ;

        assertThat(tokens.nextPosition().index()).isEqualTo(31);
        assertThat(tokens.nextPosition().column()).isEqualTo(1);
        tokens.consume(); // ALTER
        assertThat(tokens.nextPosition().index()).isEqualTo(37);
        assertThat(tokens.nextPosition().column()).isEqualTo(7);

    }

    @Test
    public void shouldConsumeUntilWithoutRepeats() {
        makeCaseInsensitive();
        String content = "FOO BEGIN A1 A2 A3 END BAR";
        tokens = new TokenStream(content, tokenizer, true);
        tokens.start();

        tokens.consume(); // FOO
        tokens.consume("BEGIN");
        tokens.consumeUntil("END");
        tokens.consume("END");
        tokens.consume("BAR");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldConsumeUntilWithRepeats() {
        makeCaseInsensitive();
        String content = "FOO BEGIN A1 A2 A3 BEGIN B1 B2 END A4 BEGIN C1 C2 END A5 END BAR";
        tokens = new TokenStream(content, tokenizer, true);
        tokens.start();

        tokens.consume(); // FOO
        tokens.consume("BEGIN");
        tokens.consumeUntil("END", "BEGIN");
        tokens.consume("END");
        tokens.consume("BAR");
        assertThat(tokens.hasNext()).isFalse();
    }

    @Test
    public void shouldConsumeUntilWithRepeatsAndMultipleSkipTokens() {
        makeCaseInsensitive();
        String content = "FOO BEGIN A1 A2 A3 IF B1 B2 END A4 REPEAT C1 C2 END A5 END BAR";
        tokens = new TokenStream(content, tokenizer, true);
        tokens.start();

        tokens.consume(); // FOO
        tokens.consume("BEGIN");
        tokens.consumeUntil("END", "BEGIN", "IF", "REPEAT");
        tokens.consume("END");
        tokens.consume("BAR");
        assertThat(tokens.hasNext()).isFalse();
    }
}
