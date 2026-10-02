/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

/**
 * A variant of {@link java.util.function.Supplier} whose {@code get()} method can throw a checked exception.
 *
 * @param <V> the type of the supplied value
 * @param <E> the checked exception type the supplier is allowed to throw
 */
@FunctionalInterface
public interface ThrowingSupplier<V, E extends Exception> {

    V get() throws E, InterruptedException;

}
