/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converter.kafka;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.config.Configuration;
import io.debezium.engine.converter.HeaderConverter;
import io.debezium.storage.kafka.KafkaConnectStorageAdapter;

/**
 * Adapter that wraps Kafka Connect's {@link org.apache.kafka.connect.storage.HeaderConverter}
 * to implement Debezium's {@link HeaderConverter} interface.
 *
 * @author Debezium Authors
 */
public class KafkaConnectHeaderConverterAdapter implements HeaderConverter, KafkaConnectStorageAdapter.HeaderConverter {

    private final org.apache.kafka.connect.storage.HeaderConverter delegate;

    public KafkaConnectHeaderConverterAdapter(org.apache.kafka.connect.storage.HeaderConverter delegate) {
        this.delegate = delegate;
    }

    public KafkaConnectHeaderConverterAdapter(Configuration converterConfig, String key) {
        final org.apache.kafka.connect.storage.HeaderConverter kafkaConverter = converterConfig.getInstance(key,
                org.apache.kafka.connect.storage.HeaderConverter.class);
        kafkaConverter.configure(converterConfig.asMap());
        this.delegate = kafkaConverter;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        delegate.configure(configs);
    }

    @Override
    public byte[] fromHeader(String topic, String headerKey, Object schema, Object value) {
        return delegate.fromConnectHeader(topic, headerKey, (Schema) schema, value);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public org.apache.kafka.connect.storage.HeaderConverter getDelegate() {
        return delegate;
    }
}
