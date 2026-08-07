/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.offset;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;

/**
 * An {@link OffsetBackingStore} wrapper that throws on the first {@code set()} call
 * to emulate offset storage failures. Useful for testing engine resilience.
 */
public class FailureEmulatingOffsetBackingStore implements OffsetBackingStore {

    public static final AtomicInteger counter = new AtomicInteger();

    private final OffsetBackingStore delegate;

    public FailureEmulatingOffsetBackingStore(OffsetBackingStore delegate) {
        this.delegate = delegate;
        counter.set(0);
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        return delegate.get(keys);
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
        if (counter.getAndIncrement() == 0) {
            throw new RuntimeException("Spurious offset storage failure");
        }
        return delegate.set(values, callback);
    }

    @Override
    public void configure(WorkerConfig config) {
        delegate.configure(config);
    }

    @Override
    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        return delegate.connectorPartitions(connectorName);
    }
}
