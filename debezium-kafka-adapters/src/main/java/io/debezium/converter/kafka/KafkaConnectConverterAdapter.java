/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converter.kafka;

import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;

import io.debezium.config.Configuration;
import io.debezium.engine.converter.Converter;
import io.debezium.storage.kafka.KafkaConnectStorageAdapter;

/**
 * Adapter that wraps Kafka Connect's {@link org.apache.kafka.connect.storage.Converter}
 * to implement Debezium's {@link Converter} interface.
 *
 * @author Debezium Authors
 */
public class KafkaConnectConverterAdapter implements Converter, KafkaConnectStorageAdapter.Converter {

    private final org.apache.kafka.connect.storage.Converter delegate;

    public KafkaConnectConverterAdapter(org.apache.kafka.connect.storage.Converter delegate) {
        this.delegate = delegate;
    }

    public KafkaConnectConverterAdapter(Configuration converterConfig, String key, boolean isKey) {
        final org.apache.kafka.connect.storage.Converter kafkaConverter = converterConfig.getInstance(key,
                org.apache.kafka.connect.storage.Converter.class);
        kafkaConverter.configure(converterConfig.asMap(), isKey);
        this.delegate = kafkaConverter;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public byte[] fromDebeziumData(String topic, Object headers, Object schema, Object value) {
        return delegate.fromConnectData(topic, (Headers) headers, (Schema) schema, value);
    }

    @Override
    public org.apache.kafka.connect.storage.Converter getDelegate() {
        return delegate;
    }
}
