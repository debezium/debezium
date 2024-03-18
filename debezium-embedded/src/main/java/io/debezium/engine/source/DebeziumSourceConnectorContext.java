/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;

import io.debezium.common.annotation.Incubating;

/**
 * {@link DebeziumSourceConnectorContext} holds useful objects used during the lifecycle of {@link DebeziumSourceConnector}.
 *
 * @author vjuranek
 */
@Incubating
public interface DebeziumSourceConnectorContext {

    /**
     * Returns the {@link OffsetBackingStore} used by this connector.
     * This should be used mainly for proper closing the offset backing store.
     * @return the {@link OffsetBackingStore} use by this connector.
     */
    OffsetBackingStore offsetStore();

    /**
     * Returns the {@link OffsetStorageReader} for this DebeziumConnectorContext.
     * @return the OffsetStorageReader for this connector.
     */
    OffsetStorageReader offsetStorageReader();

    /**
     * Returns the {@link OffsetStorageWriter} for this DebeziumConnectorContext.
     * @return the OffsetStorageWriter for this connector.
     */
    OffsetStorageWriter offsetStorageWriter();
}
