/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit test for {@link ByteBuffers}.
 */
public class ByteBuffersTest {

    @Test
    @FixFor("debezium/dbz#2533")
    public void shouldCompareRemainingContentOfPartiallyConsumedHeapBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put(new byte[]{ 9, 9, 3, 4 });
        buffer.flip();
        buffer.position(2); // remaining content is {3, 4}
        assertThat(ByteBuffers.equals(buffer, new byte[]{ 3, 4 })).isTrue();
        assertThat(ByteBuffers.equals(buffer, new byte[]{ 9, 9 })).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2533")
    public void heapAndDirectBuffersShouldAgreeForTheSameContent() {
        byte[] content = { 9, 9, 3, 4 };
        byte[] expected = { 3, 4 };

        ByteBuffer heap = ByteBuffer.allocate(4);
        heap.put(content);
        heap.flip();
        heap.position(2);

        ByteBuffer direct = ByteBuffer.allocateDirect(4);
        direct.put(content);
        direct.flip();
        direct.position(2);

        assertThat(ByteBuffers.equals(heap, expected)).isTrue();
        assertThat(ByteBuffers.equals(direct, expected)).isEqualTo(ByteBuffers.equals(heap, expected));
    }

    @Test
    public void shouldCompareWrappedArray() {
        assertThat(ByteBuffers.equals(ByteBuffer.wrap(new byte[]{ 1, 2, 3 }), new byte[]{ 1, 2, 3 })).isTrue();
        assertThat(ByteBuffers.equals(ByteBuffer.wrap(new byte[]{ 1, 2, 3 }), new byte[]{ 1, 2, 4 })).isFalse();
    }

    @Test
    public void shouldReturnFalseWhenLengthsDiffer() {
        assertThat(ByteBuffers.equals(ByteBuffer.wrap(new byte[]{ 1, 2 }), new byte[]{ 1, 2, 3 })).isFalse();
    }

    @Test
    public void shouldHandleNulls() {
        assertThat(ByteBuffers.equals(null, null)).isTrue();
        assertThat(ByteBuffers.equals(null, new byte[]{ 1 })).isFalse();
        assertThat(ByteBuffers.equals(ByteBuffer.wrap(new byte[]{ 1 }), null)).isFalse();
    }
}
