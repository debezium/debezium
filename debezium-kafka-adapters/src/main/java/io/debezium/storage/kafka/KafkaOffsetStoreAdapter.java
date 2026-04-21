/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStore;

/**
 * Adapter that wraps Kafka Connect's OffsetBackingStore to implement Debezium's OffsetBackingStore interface.
 * <p>
 * This allows Kafka-based offset storage implementations to work with Debezium's Kafka-free API.
 *
 * @author Debezium Authors
 */
public class KafkaOffsetStoreAdapter implements OffsetStore, KafkaStorageAdapter.OffsetBackingStore {

    private final org.apache.kafka.connect.storage.OffsetBackingStore delegate;

    public KafkaOffsetStoreAdapter(org.apache.kafka.connect.storage.OffsetBackingStore kafkaStore) {
        this.delegate = kafkaStore;
    }

    @Override
    public void configure(Configuration config) {
        // Convert Debezium Configuration to Kafka WorkerConfig
        org.apache.kafka.connect.runtime.WorkerConfig kafkaConfig = new org.apache.kafka.connect.runtime.distributed.DistributedConfig(config.asMap());
        delegate.configure(kafkaConfig);
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
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, OffsetStore.Callback<Void> callback) {
        // Convert Debezium Callback to Kafka Callback
        org.apache.kafka.connect.util.Callback<Void> kafkaCallback = callback != null ? callback::onCompletion : null;
        return delegate.set(values, kafkaCallback);
    }

    @Override
    public org.apache.kafka.connect.storage.OffsetBackingStore getDelegate() {
        return delegate;
    }
}
