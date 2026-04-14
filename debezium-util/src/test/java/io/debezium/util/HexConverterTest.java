/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Unit test for {@code HexConverter}.
 *
 * @author Gunnar Morling
 */
class HexConverterTest {

    @Test
    public void shouldConvertHexString() {
        byte[] bytes = HexConverter.convertFromHex("00010A0B0F106364657F8081FF");
        assertThat(bytes).isEqualTo(new byte[]{ 0, 1, 10, 11, 15, 16, 99, 100, 101, 127, -128, -127, -1 });
    }

    @Test
    void shouldRejectStringOfWrongLength() {
        assertThrows(IllegalArgumentException.class, () -> {
            HexConverter.convertFromHex("1");
        });
    }

    @Test
    void shouldRejectNonHexCharacter() {
        assertThrows(IllegalArgumentException.class, () -> {
            HexConverter.convertFromHex("GG");
        });
    }
}
