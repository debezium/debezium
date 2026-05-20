/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.offset;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;

/**
 * An {@link OffsetBackingStore} implementation that throws {@link InterruptedException}
 * when offset commits are called. Useful for testing engine shutdown behavior on interrupts.
 */
public class InterruptingOffsetBackingStore implements OffsetBackingStore {

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> collection) {
        return new CompletableFuture<>() {
            @Override
            public Map<ByteBuffer, ByteBuffer> get(long timeout, TimeUnit unit) {
                return new HashMap<>();
            }

            @Override
            public Map<ByteBuffer, ByteBuffer> get() {
                return new HashMap<>();
            }
        };
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> map, Callback<Void> callback) {
        return new CompletableFuture<>() {
            @Override
            public Void get() throws InterruptedException {
                throw new InterruptedException();
            }

            @Override
            public Void get(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };
    }

    @Override
    public void configure(WorkerConfig workerConfig) {
    }

    @Override
    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        return null;
    }
}
