/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.engine.spi.OffsetCommitPolicy;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;

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

    OffsetStorageReader offsetStorageReader();

    OffsetStorageWriter offsetStorageWriter();

    void configure(Configuration config);

    void stop();
}
