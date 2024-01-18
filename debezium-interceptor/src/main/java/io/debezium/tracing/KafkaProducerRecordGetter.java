/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.tracing;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import io.opentelemetry.context.propagation.TextMapGetter;

public enum KafkaProducerRecordGetter implements TextMapGetter<ProducerRecord<?, ?>> {
    INSTANCE;

    @Override
    public Iterable<String> keys(ProducerRecord<?, ?> carrier) {
        return StreamSupport.stream(carrier.headers().spliterator(), false)
                .map(Header::key)
                .collect(Collectors.toList());
    }

    @Override
    public String get(ProducerRecord<?, ?> carrier, String key) {
        if (carrier == null) {
            return null;
        }
        Header header = carrier.headers().lastHeader(key);
        if (header == null) {
            return null;
        }
        byte[] value = header.value();
        if (value == null) {
            return null;
        }
        return new String(value, StandardCharsets.UTF_8);
    }
}
