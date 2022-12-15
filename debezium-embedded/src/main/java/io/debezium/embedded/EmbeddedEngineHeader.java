/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import io.debezium.engine.Header;

public class EmbeddedEngineHeader<T> implements Header<T> {

    private final String key;
    private final T value;

    public EmbeddedEngineHeader(String key, T value) {
        this.key = key;
        this.value = value;
    }

    public EmbeddedEngineHeader(org.apache.kafka.connect.header.Header header) {
        this.key = header.key();
        this.value = (T) header.value();
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public T getValue() {
        return value;
    }
}
