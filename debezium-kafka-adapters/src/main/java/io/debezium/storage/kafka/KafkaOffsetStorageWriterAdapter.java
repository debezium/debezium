/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.connect.storage.Converter;

import io.debezium.spi.storage.OffsetStorageWriter;
import io.debezium.spi.storage.OffsetStore;

/**
 * Adapter that wraps Kafka Connect's OffsetStorageWriter to implement Debezium's OffsetStorageWriter interface.
 * <p>
 * This allows Kafka-based offset storage to work with Debezium's Kafka-free API.
 *
 * @author Debezium Authors
 */
public class KafkaOffsetStorageWriterAdapter implements OffsetStorageWriter, KafkaStorageAdapter.OffsetStorageWriter {

    private final org.apache.kafka.connect.storage.OffsetStorageWriter delegate;

    public KafkaOffsetStorageWriterAdapter(org.apache.kafka.connect.storage.OffsetStorageWriter kafkaWriter) {
        this.delegate = kafkaWriter;
    }

    public KafkaOffsetStorageWriterAdapter(KafkaStorageAdapter.OffsetBackingStore offsetStore, String namespace, Converter keyConverter, Converter valueConverter) {
        this.delegate = new org.apache.kafka.connect.storage.OffsetStorageWriter(offsetStore.getDelegate(), namespace, keyConverter, valueConverter);
    }

    @Override
    public void offset(Map<String, ?> partition, Map<String, ?> offset) {
        delegate.offset(partition, offset);
    }

    @Override
    public boolean beginFlush(long timeout, TimeUnit timeUnit) {
        try {
            return delegate.beginFlush(timeout, timeUnit);
        }
        catch (InterruptedException | TimeoutException e) {
            return false;
        }
    }

    @Override
    public void cancelFlush() {
        delegate.cancelFlush();
    }

    @Override
    public Future<Void> doFlush(OffsetStore.Callback<Void> callback) {
        // Convert Debezium Callback to Kafka Callback
        org.apache.kafka.connect.util.Callback<Void> kafkaCallback = callback != null ? callback::onCompletion : null;
        return delegate.doFlush(kafkaCallback);
    }

    @Override
    public org.apache.kafka.connect.storage.OffsetStorageWriter getDelegate() {
        return delegate;
    }
}
