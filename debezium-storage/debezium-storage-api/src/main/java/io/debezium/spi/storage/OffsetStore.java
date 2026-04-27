/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.storage;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.connect.runtime.WorkerConfig;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;

/**
 * Storage abstraction for connector offsets.
 * <p>
 * This interface defines the contract for persistent storage of connector offset data.
 * Implementations must be thread-safe and handle concurrent access appropriately.
 *
 * @author Debezium Authors
 */
@Incubating
public interface OffsetStore {

    /**
     * Configure the offset backing store with the provided configuration.
     * <p>
     * This method is called once before {@link #start()}.
     *
     * @param config the configuration
     */
    void configure(Configuration config);

    /**
     * Configure the offset backing store with Kafka WorkerConfig.
     * <p>
     * This method is called once before {@link #start()}.
     *
     * This method is deprecated and should be removed once Debezium config is not dependent on Kafka config.
     *
     * @param config the configuration
     */
    @Deprecated
    default void configure(WorkerConfig config) {
        configure(Configuration.from(config.values()));
    }

    /**
     * Start the offset backing store.
     * <p>
     * This method should initialize any resources needed for the backing store,
     * such as database connections, file handles, messaging producers/consumers etc.
     * <p>
     * Called after {@link #configure(Configuration)}.
     */
    void start();

    /**
     * Stop the offset backing store and release all resources.
     * <p>
     * This method should cleanly shut down the backing store, ensuring all pending
     * operations are completed or canceled.
     */
    void stop();

    /**
     * Get the values for the specified keys.
     * <p>
     * The operation is asynchronous and returns immediately with a Future.
     *
     * @param keys the keys to retrieve
     * @return a Future containing a map of key-value pairs for the requested keys
     */
    Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys);

    /**
     * Set the values for the specified key-value pairs.
     * <p>
     * The operation is asynchronous and returns immediately with a Future.
     * The callback will be invoked when the operation completes.
     *
     * @param values the key-value pairs to store
     * @param callback optional callback to invoke on completion (may be null)
     * @return a Future that completes when the values are persisted
     */
    Future<Void> set(Map<ByteBuffer, ByteBuffer> values, OffsetStore.Callback<Void> callback);

    /**
     * Callback interface for asynchronous operations in offset storage.
     *
     * @param <T> The type of the result
     */
    @FunctionalInterface
    interface Callback<T> {

        /**
         * Called when the operation completes, either successfully or with an error.
         *
         * @param error The error that occurred, or null if the operation succeeded
         * @param result The result of the operation, or null if it failed
         */
        void onCompletion(Throwable error, T result);
    }
}
