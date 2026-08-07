/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.offset;

import java.util.Map;

import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.storage.kafka.KafkaConnectOffsetStoreAdapter;

/**
 * A class containing auxiliary methods related to the Kafka Connect offsets.
 *
 * @author Debezium Authors
 */
public class KafkaConnectOffsetUtil {

    /**
     * Creates a Kafka Connect offset store and wraps it in a Debezium OffsetStore adapter.
     * This is the legacy fallback path for backward compatibility with Kafka Connect offset stores.
     */
    public static OffsetStore createKafkaOffsetStoreWithAdapter(ClassLoader classLoader, String offsetStoreClassName, Map<String, String> connectorConfig)
            throws Exception {

        // Kafka 3.5 no longer provides offset stores with non-parametric constructors
        if (offsetStoreClassName.equals(MemoryOffsetBackingStore.class.getName())) {
            return (new KafkaMemoryOffsetProvider()).create();
        }
        else if (offsetStoreClassName.equals(FileOffsetBackingStore.class.getName())) {
            return (new KafkaFileOffsetProvider()).create(Configuration.from(connectorConfig));
        }
        else if (offsetStoreClassName.equals(KafkaOffsetBackingStore.class.getName())) {
            return (new KafkaOffsetStoreProvider()).create(Configuration.from(connectorConfig));
        }
        else {
            final Class<? extends OffsetBackingStore> offsetStoreClass = (Class<OffsetBackingStore>) classLoader.loadClass(offsetStoreClassName);
            final org.apache.kafka.connect.storage.OffsetBackingStore kafkaStore = offsetStoreClass.getDeclaredConstructor().newInstance();
            return new KafkaConnectOffsetStoreAdapter(kafkaStore,
                    KafkaOffsetStoreConverter.jsonConverter(true),
                    KafkaOffsetStoreConverter.jsonConverter(false));
        }
    }

}
