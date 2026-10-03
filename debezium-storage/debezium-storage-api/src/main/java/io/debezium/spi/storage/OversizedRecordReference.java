/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.storage;

import java.net.URI;
import java.util.Objects;

import io.debezium.common.annotation.Incubating;
import io.debezium.util.Strings;

/**
 * Durable reference returned after an oversized record is stored.
 *
 * @param storage stable identifier for the storage implementation
 * @param uri absolute URI from which the complete record can be recovered
 * @param sizeBytes number of bytes stored
 *
 * @author Debezium Authors
 */
@Incubating
public record OversizedRecordReference(String storage, URI uri, long sizeBytes) {

    public OversizedRecordReference {
        Objects.requireNonNull(storage, "storage must not be null");
        if (Strings.isNullOrBlank(storage)) {
            throw new IllegalArgumentException("storage must not be blank");
        }
        Objects.requireNonNull(uri, "uri must not be null");
        if (!uri.isAbsolute()) {
            throw new IllegalArgumentException("uri must be absolute");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be non-negative");
        }
    }
}
