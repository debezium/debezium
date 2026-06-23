/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

@FunctionalInterface
public interface ThrowingRunnable<E extends Exception> {

    void run() throws E, InterruptedException;

}
