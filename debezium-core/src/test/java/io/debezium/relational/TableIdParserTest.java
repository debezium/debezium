/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * Unit test for {@link TableIdParser}.
 *
 * @author Gunnar Morling
 */
public class TableIdParserTest {

    @Test
    public void canParseValidIdentifiers() {
        assertThat(TableIdParser.parse("s.a")).containsExactly("s", "a");
        assertThat(TableIdParser.parse("s2.a")).containsExactly("s2", "a");
        assertThat(TableIdParser.parse("table")).containsExactly("table");
        assertThat(TableIdParser.parse("  table  ")).containsExactly("table");
        assertThat(TableIdParser.parse("schema.table")).containsExactly("schema", "table");
        assertThat(TableIdParser.parse("  schema  .  table  ")).containsExactly("schema", "table");
        assertThat(TableIdParser.parse("catalog.schema.table")).containsExactly("catalog", "schema", "table");
        assertThat(TableIdParser.parse("catalog  .  schema  .  table")).containsExactly("catalog", "schema", "table");
        assertThat(TableIdParser.parse("\"table\"")).containsExactly("table");
        assertThat(TableIdParser.parse("\"ta.ble\"")).containsExactly("ta.ble");
        assertThat(TableIdParser.parse("\"ta   ble\"")).containsExactly("ta   ble");
        assertThat(TableIdParser.parse("\"schema\".\"table\"")).containsExactly("schema", "table");
        assertThat(TableIdParser.parse("\"cata . log\" . \"sche . ma\" . \"ta . ble\"")).containsExactly("cata . log", "sche . ma", "ta . ble");
        assertThat(TableIdParser.parse("\"tab\"\"le\"")).containsExactly("tab\"le");
        assertThat(TableIdParser.parse("\"tab\"\"\"\"le\"")).containsExactly("tab\"\"le");
        assertThat(TableIdParser.parse("\"\"\"s\"\"\".\"\"\"a\"\"\"")).containsExactly("\"s\"", "\"a\"");
        assertThat(TableIdParser.parse("[db].[table]", new TestTableIdPredicates())).containsExactly("db", "table");
        assertThat(TableIdParser.parse("[db].[table with spaces]", new TestTableIdPredicates())).containsExactly("db", "table with spaces");
    }

    @Test(expected = IllegalArgumentException.class)
    public void leadingSeparatorIsInvalid() {
        TableIdParser.parse(".table");
    }

    @Test(expected = IllegalArgumentException.class)
    public void trailingSeparatorIsInvalid() {
        TableIdParser.parse("table.");
    }

    @Test(expected = IllegalArgumentException.class)
    public void unclosedQuotingCharIsInvalid() {
        TableIdParser.parse("\"table");
    }

    @Test(expected = IllegalArgumentException.class)
    public void escapedQuoteDoesntCloseQuotedIdentifier() {
        TableIdParser.parse("\"table\"\"");
    }

    private static class TestTableIdPredicates implements TableIdPredicates {
        @Override
        public boolean isStartDelimiter(char c) {
            return c == '[';
        }

        @Override
        public boolean isEndDelimiter(char c) {
            return c == ']';
        }
    }
}
