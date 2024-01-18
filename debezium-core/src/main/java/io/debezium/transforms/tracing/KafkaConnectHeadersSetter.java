/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.tracing;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Headers;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum KafkaConnectHeadersSetter implements TextMapSetter<Headers> {
    INSTANCE;

    KafkaConnectHeadersSetter() {
    }

    public void set(Headers headers, String key, String value) {
        if (Objects.nonNull(headers)) {
            headers.remove(key).add(key, value, Schema.STRING_SCHEMA);
        }
    }
}
