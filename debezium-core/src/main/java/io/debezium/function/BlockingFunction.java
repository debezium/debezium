/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

import java.util.Objects;
import java.util.function.Function;

/**
 * A variant of {@link Function} that can be blocked and interrupted.
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function* @author jcechace
 */
@FunctionalInterface
public interface BlockingFunction<T, R> {

    R apply(T t) throws InterruptedException;

    default <V> BlockingFunction<T, V> andThen(BlockingFunction<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return (T t) -> after.apply(apply(t));
    }

    static <T> BlockingFunction<T, T> identity() {
        return t -> t;
    }
}
