/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

import io.debezium.annotation.ThreadSafe;

/**
 * ByteBuffers-related utility methods.
 *
 * @author Shyama Praveena S
 */
@ThreadSafe
public final class ByteBuffers {

    /**
     * Compares ByteBuffer with byte array.
     *
     * @param buffer the buffer to compare
     * @param array the byte array to compare
     * @return true if the content of buffer matches the array, false otherwise
     */
    public static boolean equals(ByteBuffer buffer, byte[] array) {
        // Null checks
        if (buffer == null && array == null) {
            return true;
        }
        if (buffer == null || array == null) {
            return false;
        }

        // Length check
        if (buffer.remaining() != array.length) {
            return false;
        }

        // For HeapByteBuffer, use the backing array directly
        if (buffer.hasArray()) {
            byte[] bufferArray = buffer.array();
            int bufferOffset = buffer.arrayOffset();
            int bufferLength = buffer.remaining();

            // Compare the relevant portion of the buffer's backing array
            return Arrays.equals(
                    bufferArray, bufferOffset, bufferOffset + bufferLength,
                    array, 0, array.length);
        }

        // For direct buffers, use element-by-element comparison
        final int originalPosition = buffer.position();
        for (int i = 0; i < array.length; i++) {
            if (buffer.get(originalPosition + i) != array[i]) {
                return false;
            }
        }
        return true;
    }

}
