/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link ExtendedStringWriteParser}, using the statements as LogMiner supplies them.
 *
 * @author Chris Cranford
 */
public class ExtendedStringWriteParserTest {

    @Test
    @FixFor("debezium/dbz#3107")
    public void shouldParseStringValue() {
        assertThat(ExtendedStringWriteParser.parse(write("buf_c", "'START-vvv-END'"))).isEqualTo("START-vvv-END");
    }

    @Test
    @FixFor("debezium/dbz#3107")
    public void shouldParseNationalStringValue() {
        assertThat(ExtendedStringWriteParser.parse(write("buf_nc", "''START-nnn-END''"))).isEqualTo("START-nnn-END");
    }

    @Test
    @FixFor("debezium/dbz#3107")
    public void shouldParseValuesWithSingleQuotes() {
        // Single quotes of the value are not escaped
        assertThat(ExtendedStringWriteParser.parse(write("buf_c", "'it's-a''b;c-END'"))).isEqualTo("it's-a''b;c-END");
        assertThat(ExtendedStringWriteParser.parse(write("buf_nc", "''it's-a''b;c-END''"))).isEqualTo("it's-a''b;c-END");
    }

    @Test
    @FixFor("debezium/dbz#3107")
    public void shouldParseValuesThatStartAndEndWithSingleQuote() {
        assertThat(ExtendedStringWriteParser.parse(write("buf_c", "''quoted''"))).isEqualTo("'quoted'");
        assertThat(ExtendedStringWriteParser.parse(write("buf_nc", "'''quoted'''"))).isEqualTo("'quoted'");
    }

    @Test
    @FixFor("debezium/dbz#3107")
    public void shouldParseEmptyValues() {
        assertThat(ExtendedStringWriteParser.parse(write("buf_c", "''"))).isEmpty();
        assertThat(ExtendedStringWriteParser.parse(write("buf_nc", "''''"))).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#3107")
    public void shouldFailWhenStatementIsNotAnExtendedStringWrite() {
        assertThatThrownBy(() -> ExtendedStringWriteParser.parse("update \"DEBEZIUM\".\"T\" set \"A\" = 'b';"))
                .isInstanceOf(DebeziumException.class);
    }

    private static String write(String buffer, String literal) {
        return "\n " + buffer + " := " + literal + "; \n DBMS_LOB.WriteAppend(TempLob, LENGTH(" + buffer + "), " + buffer + "); \n ";
    }
}
