/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

/**
 * Tests for the {@code readString} method in {@link PgOutputMessageDecoder}.
 * Verifies correct decoding of null-terminated UTF-8 strings from a ByteBuffer,
 * including multi-byte characters used in non-ASCII table/column names.
 */
public class PgOutputMessageDecoderReadStringTest {

    private static String invokeReadString(ByteBuffer buffer) throws Exception {
        Method method = PgOutputMessageDecoder.class.getDeclaredMethod("readString", ByteBuffer.class);
        method.setAccessible(true);
        return (String) method.invoke(null, buffer);
    }

    private static ByteBuffer toNullTerminatedBuffer(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 1);
        buffer.put(bytes);
        buffer.put((byte) 0); // null terminator
        buffer.flip();
        return buffer;
    }

    @Test
    public void shouldDecodeAsciiString() throws Exception {
        ByteBuffer buffer = toNullTerminatedBuffer("test_table");
        String result = invokeReadString(buffer);
        assertThat(result).isEqualTo("test_table");
    }

    @Test
    public void shouldDecodeEmptyString() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 0);
        buffer.flip();
        String result = invokeReadString(buffer);
        assertThat(result).isEmpty();
    }

    @Test
    public void shouldDecodeTwoByteUtf8Characters() throws Exception {
        // "café" — é is 2 bytes in UTF-8 (0xC3 0xA9)
        ByteBuffer buffer = toNullTerminatedBuffer("café");
        String result = invokeReadString(buffer);
        assertThat(result).isEqualTo("café");
    }

    @Test
    public void shouldDecodeThreeByteUtf8Characters() throws Exception {
        // Simulates a schema-qualified CJK table name; each CJK character is 3 bytes in UTF-8
        ByteBuffer buffer = toNullTerminatedBuffer("schema_名.test_表");
        String result = invokeReadString(buffer);
        assertThat(result).isEqualTo("schema_名.test_表");
    }

    @Test
    public void shouldDecodeFourByteUtf8Characters() throws Exception {
        // "table_🎉" — 🎉 is 4 bytes in UTF-8 (0xF0 0x9F 0x8E 0x89)
        ByteBuffer buffer = toNullTerminatedBuffer("table_🎉");
        String result = invokeReadString(buffer);
        assertThat(result).isEqualTo("table_🎉");
    }

    @Test
    public void shouldOnlyConsumeUpToNullTerminator() throws Exception {
        // Buffer contains two null-terminated strings; readString should only consume the first
        byte[] first = "first".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(first.length + 1 + second.length + 1);
        buffer.put(first);
        buffer.put((byte) 0);
        buffer.put(second);
        buffer.put((byte) 0);
        buffer.flip();

        String result = invokeReadString(buffer);
        assertThat(result).isEqualTo("first");
        // Buffer position should be right after the first null terminator
        assertThat(buffer.remaining()).isEqualTo(second.length + 1);
    }
}
