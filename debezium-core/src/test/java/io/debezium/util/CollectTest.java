/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Set;

import org.junit.Test;

/**
 * Unit test for {@link Collect}.
 *
 * @author Gunnar Morling
 */
public class CollectTest {

    @Test
    public void unmodifiableSetForIteratorShouldReturnExpectedElements() {
        Set<Integer> values = Collect.unmodifiableSet(Arrays.asList(1, 2, 3, 42).iterator());
        assertThat(values).containsOnly(1, 2, 3, 42);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unmodifiableSetForIteratorShouldRaiseExceptionUponModification() {
        Set<Integer> values = Collect.unmodifiableSet(Arrays.asList(1, 2, 3, 42).iterator());
        values.remove(1);
    }
}
