/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

public class QueryEventDataWithTimeZoneDeserializerTest {

    @Test
    @FixFor("debezium/dbz#2866")
    void shouldReadTimeZoneAfterPrecedingStatusVariables() {
        // In the order in which MySQL writes them: flags, sql_mode, catalog, charset, time zone, and a later variable
        final byte[] statusVariables = statusVariables(
                bytes(0, 0, 0, 0, 0),
                bytes(1, 0, 0, 0, 0, 0, 0, 0, 0),
                variable(6, "std"),
                bytes(4, 33, 0, 33, 0, 255, 0),
                variable(5, "+09:00"),
                bytes(17, 1, 2, 3, 4, 5, 6, 7, 8));

        assertThat(QueryEventDataWithTimeZoneDeserializer.readTimeZone(statusVariables)).isEqualTo("+09:00");
    }

    @Test
    @FixFor("debezium/dbz#2866")
    void shouldNotReadTimeZoneThatIsMissing() {
        assertThat(QueryEventDataWithTimeZoneDeserializer.readTimeZone(statusVariables(
                bytes(0, 0, 0, 0, 0),
                bytes(4, 33, 0, 33, 0, 255, 0)))).isNull();
        assertThat(QueryEventDataWithTimeZoneDeserializer.readTimeZone(new byte[0])).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2866")
    void shouldStopAtUnknownStatusVariable() {
        // The length of an unknown variable is not known, so the time zone after it cannot be found
        assertThat(QueryEventDataWithTimeZoneDeserializer.readTimeZone(statusVariables(
                bytes(99, 1, 2, 3),
                variable(5, "SYSTEM")))).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2866")
    void shouldNotReadTruncatedTimeZone() {
        final byte[] truncated = { 5, 6, '+', '0', '9' };
        assertThat(QueryEventDataWithTimeZoneDeserializer.readTimeZone(truncated)).isNull();
    }

    private static byte[] statusVariables(byte[]... variables) {
        final var output = new ByteArrayOutputStream();
        for (byte[] variable : variables) {
            output.writeBytes(variable);
        }
        return output.toByteArray();
    }

    private static byte[] variable(int code, String value) {
        final byte[] text = value.getBytes(StandardCharsets.US_ASCII);
        final byte[] variable = new byte[text.length + 2];
        variable[0] = (byte) code;
        variable[1] = (byte) text.length;
        System.arraycopy(text, 0, variable, 2, text.length);
        return variable;
    }

    private static byte[] bytes(int... values) {
        final byte[] bytes = new byte[values.length];
        for (int index = 0; index < values.length; index++) {
            bytes[index] = (byte) values[index];
        }
        return bytes;
    }
}
