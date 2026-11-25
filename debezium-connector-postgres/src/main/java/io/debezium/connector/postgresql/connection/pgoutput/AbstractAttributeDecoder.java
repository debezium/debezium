/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A class to contain shared code between text and binary attribute decoders
 */
public abstract class AbstractAttributeDecoder implements AttributeDecoder {

    /**
     * Decodes a UTF-8 string from a heap-buffer.
     *
     * @param buffer a heap-buffer to read the string from
     * @param length number of bytes to read.
     * @return decoded string
     */
    protected static String decodeStringOfLength(final ByteBuffer buffer, final int length) {
        final var value = new String(buffer.array(),
                buffer.arrayOffset() + buffer.position(),
                length,
                StandardCharsets.UTF_8);
        buffer.position(buffer.position() + length);
        return value;
    }

    /**
     * Reads the replication stream where the column stream specifies a length followed by the value.
     *
     * @param buffer The replication stream buffer
     * @return the column value as a string read from the replication stream
     */
    protected static String decodeString(final ByteBuffer buffer) {
        final var length = buffer.getInt();
        if (length < 0 || length > buffer.remaining()) {
            throw new BufferUnderflowException();
        }

        return decodeStringOfLength(buffer, length);
    }
}
