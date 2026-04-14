/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link Collect}.
 *
 * @author Gunnar Morling
 */
class CollectTest {

    @Test
    public void unmodifiableSetForIteratorShouldReturnExpectedElements() {
        Set<Integer> values = Collect.unmodifiableSet(Arrays.asList(1, 2, 3, 42).iterator());
        assertThat(values).containsOnly(1, 2, 3, 42);
    }

    @Test
    void unmodifiableSetForIteratorShouldRaiseExceptionUponModification() {
        assertThrows(UnsupportedOperationException.class, () -> {
            Set<Integer> values = Collect.unmodifiableSet(Arrays.asList(1, 2, 3, 42).iterator());
            values.remove(1);
        });
    }
}
