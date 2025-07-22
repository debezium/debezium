/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.doc.FixFor;

public class TableIdTest {

    @Test
    @FixFor("DBZ-3057")
    public void shouldDoubleQuoteTableId() {
        // Non-quoted table-id
        TableId id = new TableId("catalog", "schema", "table");

        TableId doubleQuoted = id.toDoubleQuoted();
        assertThat(doubleQuoted.catalog()).isEqualTo("\"catalog\"");
        assertThat(doubleQuoted.schema()).isEqualTo("\"schema\"");
        assertThat(doubleQuoted.table()).isEqualTo("\"table\"");
        assertThat(doubleQuoted.toString()).isEqualTo("\"catalog\".\"schema\".\"table\"");
        assertThat(doubleQuoted.toDoubleQuotedString()).isEqualTo("\"catalog\".\"schema\".\"table\"");

        // Quoted table-id
        id = new TableId("\"catalog\"", "\"schema\"", "\"table\"");

        doubleQuoted = id.toDoubleQuoted();
        assertThat(doubleQuoted).isEqualTo(id);
    }

    @Test
    @FixFor("DBZ-3057")
    public void shouldQuoteTableIdWithGivenCharacter() {
        // Non-quoted quoted table-id
        TableId id = new TableId("catalog", "schema", "table");

        TableId quoted = id.toQuoted('\'', '\'');
        assertThat(quoted.catalog()).isEqualTo("'catalog'");
        assertThat(quoted.schema()).isEqualTo("'schema'");
        assertThat(quoted.table()).isEqualTo("'table'");
        assertThat(quoted.toString()).isEqualTo("'catalog'.'schema'.'table'");
        assertThat(quoted.toDoubleQuotedString()).isEqualTo("\"'catalog'\".\"'schema'\".\"'table'\"");

        // Quoted table-id
        id = new TableId("'catalog'", "'schema'", "'table'");

        quoted = id.toQuoted('\'', '\'');
        assertThat(quoted).isEqualTo(id);
    }

    @Test
    public void shouldQuoteTableIdWithBracket() {
        // Non-quoted quoted table-id
        TableId id = new TableId("cata log", "sch ema", "ta b$le");

        TableId quoted = id.toBracketQuoted();
        assertThat(quoted.catalog()).isEqualTo("[cata log]");
        assertThat(quoted.schema()).isEqualTo("[sch ema]");
        assertThat(quoted.table()).isEqualTo("[ta b$le]");
        assertThat(quoted.toString()).isEqualTo("[cata log].[sch ema].[ta b$le]");
        assertThat(quoted.toDoubleQuotedString()).isEqualTo("\"[cata log]\".\"[sch ema]\".\"[ta "
                + "b$le]\"");

        // Quoted table-id
        id = new TableId("[cata log]", "[sch ema]", "[ta b$le]");

        quoted = id.toBracketQuoted();
        assertThat(quoted).isEqualTo(id);
    }
}
