/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * An adaptation of {@link Consumer} which allows for exceptions to passed through
 * @author Jakub Cechacek
 */
@FunctionalInterface
public interface Commands<T, E extends Throwable> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @throws E possibly thrown exception
     */
    void execute(T t) throws E;

    /**
     * See {@link Consumer}
     * @param after the operation to perform after this operation
     * @return a composed {@code Consumer} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws NullPointerException if {@code after} is null
     */
    default Commands<T, E> andThen(Commands<? super T, E> after) {
        Objects.requireNonNull(after);
        return (T t) -> {
            execute(t);
            after.execute(t);
        };
    }
}