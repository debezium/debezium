/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.util;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class KeyStringDecoderTest {

    @Test
    public void testGetOrNull_WithinBounds() {
        List<Object> testList = Arrays.asList("test1", "test2", "test3");
        assertThat("test1").isEqualTo((String) KeyStringDecoder.getOrNull(testList, 0));
        assertThat("test2").isEqualTo((String) KeyStringDecoder.getOrNull(testList, 1));
        assertThat("test3").isEqualTo((String) KeyStringDecoder.getOrNull(testList, 2));
    }

    @Test
    public void testGetOrNull_OutOfBounds() {
        List<Object> testList = Arrays.asList("test1", "test2", "test3");
        Object result = KeyStringDecoder.getOrNull(testList, 3);
        assertThat(result).isNull();
    }

    @Test
    public void testGetOrNull_EmptyList() {
        List<Object> testList = List.of();
        Object result = KeyStringDecoder.getOrNull(testList, 0);
        assertThat(result).isNull();
    }

    @Test
    public void testBufferConsumer32BitMask() {
        assertThat(KeyStringDecoder.BufferConsumer.MASK_32_BIT).isEqualTo((1L << 32) - 1);
    }
}
