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
import io.debezium.connector.oracle.logminer.parser.LobWriteParser.LobWrite;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link LobWriteParser}, using the statements as LogMiner supplies them.
 *
 * @author Chris Cranford
 */
public class LobWriteParserTest {

    @Test
    @FixFor("debezium/dbz#3109")
    public void shouldParseClobWrite() {
        final LobWrite write = LobWriteParser.parse("  buf_c := 'Test data';\n  dbms_lob.write(loc_c, 9, 1, buf_c);");
        assertThat(write.data()).isEqualTo("Test data");
        assertThat(write.length()).isEqualTo(9);
        assertThat(write.offset()).isEqualTo(0);
    }

    @Test
    @FixFor("debezium/dbz#3109")
    public void shouldParseBlobWrite() {
        final LobWrite write = LobWriteParser.parse("  buf_b := HEXTORAW('ff4a5a01');\n  dbms_lob.write(loc_b, 4, 2049, buf_b);");
        assertThat(write.data()).isEqualTo("HEXTORAW('ff4a5a01')");
        assertThat(write.length()).isEqualTo(4);
        assertThat(write.offset()).isEqualTo(2048);
    }

    @Test
    @FixFor("debezium/dbz#3109")
    public void shouldParseJsonWrite() {
        // LogMiner marks the buffer and the locator of a JSON column with a hint
        final LobWrite write = LobWriteParser.parse("  buf_b := /* JSON */ HEXTORAW('ff4a5a01');\n  dbms_lob.write(loc_b /* JSON */, 4, 2049, buf_b);");
        assertThat(write.data()).isEqualTo("HEXTORAW('ff4a5a01')");
        assertThat(write.length()).isEqualTo(4);
        assertThat(write.offset()).isEqualTo(2048);
    }

    @Test
    @FixFor("debezium/dbz#3109")
    public void shouldFailWhenStatementIsNotLobWrite() {
        assertThatThrownBy(() -> LobWriteParser.parse("update \"DEBEZIUM\".\"T\" set \"A\" = 'b';"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Unable to parse unsupported LOB_WRITE SQL");
    }
}
