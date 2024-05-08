/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Iterator;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import io.debezium.annotation.Immutable;

/**
 * Utility methods for obtaining streams of integers.
 *
 * @author Randall Hauch
 */
@Immutable
public class Sequences {

    /**
     * Create a stream of <em>number</em> monotonically increasing numbers starting at 0, useful when performing an operation
     * <em>number</em> times.
     *
     * @param number the number of values to include in the stream; must be positive
     * @return the sequence; never null
     */
    public static IntStream times(int number) {
        return IntStream.range(0, number);
    }

    /**
     * Create an iterator over an infinite number of monotonically increasing numbers starting at 0, useful when performing an
     * operation an unknown number of times.
     *
     * @return the sequence; never null
     */
    public static Iterable<Integer> infiniteIntegers() {
        return infiniteIntegers(0);
    }

    /**
     * Create an iterator over an infinite number monotonically increasing numbers starting at the given number, useful when
     * performing an operation an unknown number of times.
     *
     * @param startingAt the first number to include in the resulting stream
     * @return the sequence; never null
     */
    public static Iterable<Integer> infiniteIntegers(int startingAt) {
        return Iterators.around(new Iterator<Integer>() {
            private int counter = startingAt;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Integer next() {
                return Integer.valueOf(counter++);
            }
        });
    }

    /**
     * Obtain a supplier function that randomly selects from the given values. If the supplied values contain nulls, then
     * the resulting supplier function may return null values.
     *
     * @param first the first value that may be randomly picked
     * @param additional the additional values to randomly pick from; may be null or empty
     * @return the supplier function; never null
     */
    @SafeVarargs
    public static <T> Supplier<T> randomlySelect(T first, T... additional) {
        if (additional == null || additional.length == 0) {
            return () -> first;
        }
        Random rng = new Random(System.currentTimeMillis());
        int max = additional.length + 1;
        return () -> {
            int index = rng.nextInt(max);
            return index == 0 ? first : additional[index - 1];
        };
    }

    /**
     * Obtain a supplier function that randomly selects from the given values. If the supplied values contain nulls, then
     * the resulting supplier function may return null values.
     *
     * @param values the values to randomly pick from; may not be null, should not be empty
     * @return the supplier function; never null
     */
    @SafeVarargs
    public static <T> Supplier<T> randomlySelect(T... values) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("The values array may not be null or empty");
        }
        Random rng = new Random(System.currentTimeMillis());
        return () -> values[rng.nextInt(values.length)];
    }
}
