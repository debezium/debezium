/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Instantiator;

/**
 * Default implementation of {@link OffsetManager} that uses Kafka's OffsetStorageWriter to commit offsets.
 */
@ThreadSafe
public class KafkaOffsetManager implements OffsetManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaOffsetManager.class);

    private final OffsetBackingStore offsetBackingStore;
    private final OffsetStorageWriter offsetStorageWriter;
    private final OffsetStorageReader offsetStorageReader;

    public KafkaOffsetManager(Configuration config) {
        this(initializeOffsetStore(config), config);
    }

    public KafkaOffsetManager(OffsetBackingStore offsetBackingStore, Configuration config) {
        final String engineName = config.getString(EmbeddedEngine.ENGINE_NAME);

        Map<String, String> internalConverterConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");

        Converter keyConverter = Instantiator.getInstance(JsonConverter.class.getName());
        keyConverter.configure(internalConverterConfig, true);

        Converter valueConverter = Instantiator.getInstance(JsonConverter.class.getName());
        valueConverter.configure(internalConverterConfig, false);

        this.offsetBackingStore = offsetBackingStore;
        this.offsetStorageWriter = new OffsetStorageWriter(offsetBackingStore, engineName, keyConverter, valueConverter);
        this.offsetStorageReader = new OffsetStorageReaderImpl(offsetBackingStore, engineName, keyConverter, valueConverter);
    }

    public OffsetBackingStore getOffsetBackingStore() {
        return offsetBackingStore;
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return offsetStorageReader;
    }

    @Override
    public OffsetStorageWriter offsetStorageWriter() {
        return offsetStorageWriter;
    }

    @Override
    public void start() {
        this.offsetBackingStore.start();
    }

    @Override
    public void stop() {
        this.offsetBackingStore.stop();
    }

    private static OffsetBackingStore initializeOffsetStore(Configuration config) {
        LOGGER.info("Initializing OffsetBackingStore from configuration");
        // Instantiate the offset store ...
        final String offsetStoreClassName = config.getString(OFFSET_STORAGE);
        OffsetBackingStore offsetBackingStore = Instantiator.getInstance(offsetStoreClassName);

        // Initialize the offset store ...
        try {
            Map<String, String> embeddedConfig = config.asMap(Field.setOf());
            embeddedConfig.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
            embeddedConfig.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
            WorkerConfig workerConfig = new EmbeddedEngine.EmbeddedConfig(embeddedConfig);

            offsetBackingStore.configure(workerConfig);
        }
        catch (Throwable t) {
            offsetBackingStore.stop();
            throw new RuntimeException(t);

        }
        return offsetBackingStore;
    }
}
