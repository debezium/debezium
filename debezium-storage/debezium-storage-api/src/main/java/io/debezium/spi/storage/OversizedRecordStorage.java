/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.storage;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;

/**
 * Storage contract used by record-size enforcement to externalize complete records.
 * <p>
 * Implementations must return from {@link #write(OversizedRecord)} only after the
 * payload is durably acknowledged. A write failure must be propagated to the caller
 * so that no claim-check marker is emitted for a missing payload.
 * Implementations must also treat {@link OversizedRecord#key()} as idempotent: writing
 * the same key and payload more than once must be safe.
 *
 * @author Debezium Authors
 */
@Incubating
public interface OversizedRecordStorage extends AutoCloseable {

    /**
     * Configures the storage implementation.
     *
     * @param properties implementation-specific configuration
     */
    void configure(Configuration properties);

    /**
     * Writes an oversized record and returns its durable external reference.
     *
     * @param record serialized record and deterministic key
     * @return durable external reference
     */
    OversizedRecordReference write(OversizedRecord record);

    @Override
    default void close() {
    }
}
