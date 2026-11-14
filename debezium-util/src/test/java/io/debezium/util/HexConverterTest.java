/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
    void shouldConvertCharArray() {
        byte[] bytes = HexConverter.convertFromHex("00010A0B0F106364657F8081FF".toCharArray());
        assertThat(bytes).isEqualTo(new byte[]{ 0, 1, 10, 11, 15, 16, 99, 100, 101, 127, -128, -127, -1 });
    }

    @Test
    void shouldConvertToHexStringAndBack() {
        byte[] original = new byte[]{ 0, 1, 10, 11, 15, 16, 99, 100, 101, 127, -128, -127, -1 };
        String hexString = HexConverter.convertToHexString(original);
        assertThat(hexString).isEqualTo("00010a0b0f106364657f8081ff");
        assertThat(HexConverter.convertFromHex(hexString)).isEqualTo(original);
    }

    @Test
    void shouldConvertToHexBytesAndBack() {
        byte[] original = new byte[]{ 0, 1, 10, 15, -1 };
        byte[] hexBytes = HexConverter.convertToHexBytes(original);
        assertThat(hexBytes).isEqualTo(new byte[]{ '0', '0', '0', '1', '0', 'a', '0', 'f', 'f', 'f' });
    }

    @Test
    void shouldHandleEmptyArrays() {
        assertThat(HexConverter.convertToHexString(new byte[0])).isEqualTo("");
        assertThat(HexConverter.convertToHexBytes(new byte[0])).isEqualTo(new byte[0]);
        assertThat(HexConverter.convertFromHex("")).isEqualTo(new byte[0]);
        assertThat(HexConverter.convertFromHex(new char[0])).isEqualTo(new byte[0]);
    }

    @Test
    void shouldRejectNullInputs() {
        assertThrows(NullPointerException.class, () -> HexConverter.convertToHexString(null));
        assertThrows(NullPointerException.class, () -> HexConverter.convertToHexBytes(null));
        assertThrows(NullPointerException.class, () -> HexConverter.convertFromHex((String) null));
        assertThrows(NullPointerException.class, () -> HexConverter.convertFromHex((char[]) null));
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
