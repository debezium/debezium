/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;

import io.debezium.config.Field;

/**
 * Interface for committing offsets.
 */
public interface OffsetManager {

    /**
     * An optional field that specifies the name of the class that implements the {@link OffsetBackingStore} interface,
     * and that will be used to store offsets recorded by the connector.
     */
    Field OFFSET_STORAGE = Field.create("offset.storage")
            .withDescription("The Java class that implements the `OffsetBackingStore` "
                    + "interface, used to periodically store offsets so that, upon "
                    + "restart, the connector can resume where it last left off.")
            .withDefault(FileOffsetBackingStore.class.getName());

    /**
     * Retrieve the {@link OffsetBackingStore} that is managed by this {@link OffsetManager}.
     */
    OffsetBackingStore getOffsetBackingStore();

    /**
     * Retrieve the {@link OffsetStorageReader} that is managed by this {@link OffsetManager}.
     */
    OffsetStorageReader offsetStorageReader();

    /**
     * Retrieve the {@link OffsetStorageWriter} that is managed by this {@link OffsetManager}.
     */
    OffsetStorageWriter offsetStorageWriter();

    /**
     * Lifecycle method indicating that processing should start.
     */
    void start();

    /**
     * Lifecycle method indicating that processing should stop and resources be freed.
     */
    void stop();
}
