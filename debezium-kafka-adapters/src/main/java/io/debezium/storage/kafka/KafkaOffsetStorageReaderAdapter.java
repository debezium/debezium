/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;

import io.debezium.spi.storage.OffsetStorageReader;

/**
 * Adapter that wraps Kafka Connect's OffsetStorageReader to implement Debezium's OffsetStorageReader interface.
 * <p>
 * This allows Kafka-based offset storage to work with Debezium's Kafka-free API.
 *
 * @author Debezium Authors
 */
public class KafkaOffsetStorageReaderAdapter implements OffsetStorageReader, KafkaStorageAdapter.OffsetStorageReader {

    private final org.apache.kafka.connect.storage.OffsetStorageReader delegate;

    public KafkaOffsetStorageReaderAdapter(org.apache.kafka.connect.storage.OffsetStorageReader kafkaReader) {
        this.delegate = kafkaReader;
    }

    public KafkaOffsetStorageReaderAdapter(KafkaStorageAdapter.OffsetBackingStore offsetStore, String namespace, Converter keyConverter, Converter valueConverter) {
        this.delegate = new OffsetStorageReaderImpl(offsetStore.getDelegate(), namespace, keyConverter, valueConverter);
    }

    @Override
    public <T> Map<String, Object> offset(Map<String, T> partition) {
        return delegate.offset(partition);
    }

    @Override
    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
        return delegate.offsets(partitions);
    }

    @Override
    public org.apache.kafka.connect.storage.OffsetStorageReader getDelegate() {
        return delegate;
    }
}
