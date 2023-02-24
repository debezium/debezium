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

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Instantiator;

/**
 * Default implementation of {@link OffsetManager} that uses Kafka's OffsetStorageWriter to commit offsets.
 * This class is meant to be used in a thread confined manner and is not thread safe.
 */
public class DefaultOffsetManager implements OffsetManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOffsetManager.class);

    private OffsetStorageWriter offsetStorageWriter;
    private OffsetBackingStore offsetStore;
    private OffsetStorageReader offsetReader;
    private Converter keyConverter;
    private Converter valueConverter;

    public void configure(Configuration config) {
        this.offsetStore = initializeOffsetStore(config);

        final String engineName = config.getString(EmbeddedEngine.ENGINE_NAME);

        Map<String, String> internalConverterConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        this.keyConverter = Instantiator.getInstance(JsonConverter.class.getName());
        this.keyConverter.configure(internalConverterConfig, true);
        this.valueConverter = Instantiator.getInstance(JsonConverter.class.getName());
        this.valueConverter.configure(internalConverterConfig, false);

        this.offsetStorageWriter = new OffsetStorageWriter(offsetStore, engineName, keyConverter, valueConverter);
        this.offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName, keyConverter, valueConverter);
    }

    private static OffsetBackingStore initializeOffsetStore(Configuration config) {
        // Instantiate the offset store ...
        final String offsetStoreClassName = config.getString(OFFSET_STORAGE);
        OffsetBackingStore offsetStore = Instantiator.getInstance(offsetStoreClassName);

        // Initialize the offset store ...
        try {
            Map<String, String> embeddedConfig = config.asMap(Field.setOf());
            embeddedConfig.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
            embeddedConfig.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
            WorkerConfig workerConfig = new EmbeddedEngine.EmbeddedConfig(embeddedConfig);

            offsetStore.configure(workerConfig);
            offsetStore.start();
        }
        catch (Throwable t) {
            offsetStore.stop();
            throw new RuntimeException(t);

        }
        return offsetStore;
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return offsetReader;
    }

    @Override
    public OffsetStorageWriter offsetStorageWriter() {
        return offsetStorageWriter;
    }

    @Override
    public void stop() {
        this.offsetStore.stop();
    }

}
