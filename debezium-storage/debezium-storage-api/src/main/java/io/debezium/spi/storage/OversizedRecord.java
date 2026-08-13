/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.storage;

import java.util.Objects;

import io.debezium.common.annotation.Incubating;

/**
 * A serialized source record that exceeds the configured inline message size.
 *
 * @param key deterministic storage key; retries of the same source record must use the same key
 * @param payload complete serialized source record
 * @param contentType media type of the serialized payload
 *
 * @author Debezium Authors
 */
@Incubating
public record OversizedRecord(String key, byte[] payload, String contentType) {

    public OversizedRecord {
        if (Objects.requireNonNull(key, "key must not be null").isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        payload = Objects.requireNonNull(payload, "payload must not be null").clone();
        if (payload.length == 0) {
            throw new IllegalArgumentException("payload must not be empty");
        }
        if (Objects.requireNonNull(contentType, "contentType must not be null").isBlank()) {
            throw new IllegalArgumentException("contentType must not be blank");
        }
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
