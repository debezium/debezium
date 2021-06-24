/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * @author Randall Hauch
 */
public class HashCodeTest {

    @Test
    public void shouldComputeHashCodeForOnePrimitive() {
        assertThat(HashCode.compute(1), is(not(0)));
        assertThat(HashCode.compute((long) 8), is(not(0)));
        assertThat(HashCode.compute((short) 3), is(not(0)));
        assertThat(HashCode.compute(1.0f), is(not(0)));
        assertThat(HashCode.compute(1.0d), is(not(0)));
        assertThat(HashCode.compute(true), is(not(0)));
    }

    @Test
    public void shouldComputeHashCodeForMultiplePrimitives() {
        assertThat(HashCode.compute(1, 2, 3), is(not(0)));
        assertThat(HashCode.compute((long) 8, (long) 22, 33), is(not(0)));
        assertThat(HashCode.compute((short) 3, (long) 22, true), is(not(0)));
    }

    @Test
    public void shouldAcceptNoArguments() {
        assertThat(HashCode.compute(), is(0));
    }

    @Test
    public void shouldAcceptNullArguments() {
        assertThat(HashCode.compute((Object) null), is(0));
        assertThat(HashCode.compute("abc", (Object) null), is(not(0)));
    }

}
