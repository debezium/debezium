/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static java.util.concurrent.CompletableFuture.runAsync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

/**
 * Utilities for working with {@link Startable}s in a MongoDB context.
 */
public final class MongoDbStartables {

    /**
     * Start every supplied {@link Startable} recursively and, to the extent possible, in parallel, waiting synchronously
     * on the result.
     *
     * @param startables the list of startables to {@link Startable#start())
     */
    public static void deepStart(Stream<? extends Startable> startables) {
        try {
            Startables.deepStart(startables).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Stop every supplied {@link Startable} recursively and, to the extent possible, in parallel, waiting synchronously
     * on the result.
     *
     * @param startables the list of startables to {@link Startable#stop())
     */
    public static void deepStop(Stream<? extends Startable> startables) {
        try {
            CompletableFuture.allOf(startables
                    .map(node -> runAsync(node::stop))
                    .toArray(CompletableFuture[]::new))
                    .get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private MongoDbStartables() {
        throw new AssertionError("Should not be instantiated");
    }

}
