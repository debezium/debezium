/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class EnumeratedValuesTest {

    @Test
    void shouldEncodeValuesAsCommaSeparatedString() {
        assertThat(EnumeratedValues.toCommaSeparatedString(Arrays.asList("a", "b,c", "d"))).isEqualTo("a,b\\,c,d");
    }

    @Test
    void shouldDecodeCommaSeparatedString() {
        assertThat(EnumeratedValues.fromCommaSeparatedString("a,b\\,c,d")).containsExactly("a", "b,c", "d");
    }

    @Test
    void shouldPreserveBackslashWhenEncodingAndDecodingCommaSeparatedString() {
        final String values = EnumeratedValues.toCommaSeparatedString(Arrays.asList("plain", "back\\slash"));

        assertThat(values).isEqualTo("plain,back\\\\slash");
        assertThat(EnumeratedValues.fromCommaSeparatedString(values)).containsExactly("plain", "back\\slash");
    }

    @Test
    void shouldPreserveBackslashBeforeCommaWhenEncodingAndDecodingCommaSeparatedString() {
        final String values = EnumeratedValues.toCommaSeparatedString(Arrays.asList("plain", "back\\,comma", "tail"));

        assertThat(EnumeratedValues.fromCommaSeparatedString(values)).containsExactly("plain", "back\\,comma", "tail");
    }

    @Test
    void shouldPreserveBackslashBeforeDelimiterWhenEncodingAndDecodingCommaSeparatedString() {
        final String values = EnumeratedValues.toCommaSeparatedString(Arrays.asList("ends\\", "next"));

        assertThat(EnumeratedValues.fromCommaSeparatedString(values)).containsExactly("ends\\", "next");
    }

    @Test
    void shouldPreserveLegacyUnescapedBackslashWhenDecodingCommaSeparatedString() {
        assertThat(EnumeratedValues.fromCommaSeparatedString("plain,back\\slash")).containsExactly("plain", "back\\slash");
    }

    @Test
    void shouldPreserveWhitespaceWhenDecodingCommaSeparatedString() {
        assertThat(EnumeratedValues.fromCommaSeparatedString(" a\\, b, c ")).containsExactly(" a, b", " c ");
    }

    @Test
    void shouldDecodeEmptyStringAsEmptyValue() {
        assertThat(EnumeratedValues.fromCommaSeparatedString("")).containsExactly("");
    }

    @Test
    void shouldDecodeNullAsEmptyList() {
        assertThat(EnumeratedValues.fromCommaSeparatedString(null)).isEmpty();
    }
}
