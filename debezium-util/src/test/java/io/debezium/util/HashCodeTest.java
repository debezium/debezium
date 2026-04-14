/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * @author Randall Hauch
 */
public class HashCodeTest {

    @Test
    public void shouldComputeHashCodeForOnePrimitive() {
        assertThat(HashCode.compute(1)).isNotEqualTo(0);
        assertThat(HashCode.compute((long) 8)).isNotEqualTo(0);
        assertThat(HashCode.compute((short) 3)).isNotEqualTo(0);
        assertThat(HashCode.compute(1.0f)).isNotEqualTo(0);
        assertThat(HashCode.compute(1.0d)).isNotEqualTo(0);
        assertThat(HashCode.compute(true)).isNotEqualTo(0);
    }

    @Test
    public void shouldComputeHashCodeForMultiplePrimitives() {
        assertThat(HashCode.compute(1, 2, 3)).isNotEqualTo(0);
        assertThat(HashCode.compute((long) 8, (long) 22, 33)).isNotEqualTo(0);
        assertThat(HashCode.compute((short) 3, (long) 22, true)).isNotEqualTo(0);
    }

    @Test
    public void shouldAcceptNoArguments() {
        assertThat(HashCode.compute()).isEqualTo(0);
    }

    @Test
    public void shouldAcceptNullArguments() {
        assertThat(HashCode.compute((Object) null)).isEqualTo(0);
        assertThat(HashCode.compute("abc", null)).isNotEqualTo(0);
    }

}
