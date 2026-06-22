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
