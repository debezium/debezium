/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * Base class for Debezium's CDC {@link SourceTask} implementations. Provides functionality common to all connectors,
 * such as validation of the configuration.
 *
 * @author Gunnar Morling
 */
public abstract class BaseSourceTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseSourceTask.class);

    @Override
    public final void start(Map<String, String> props) {
        if (context == null) {
            throw new ConnectException("Unexpected null context");
        }

        Configuration config = Configuration.from(props);
        if (!config.validateAndRecord(getAllConfigurationFields(), LOGGER::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting {} with configuration:", getClass().getSimpleName());
            config.withMaskedPasswords().forEach((propName, propValue) -> {
                LOGGER.info("   {} = {}", propName, propValue);
            });
        }

        start(config);
    }

    /**
     * Called once when starting this source task.
     *
     * @param config
     *            the task configuration; implementations should wrap it in a dedicated implementation of
     *            {@link CommonConnectorConfig} and work with typed access to configuration properties that way
     */
    protected abstract void start(Configuration config);

    /**
     * Returns all configuration {@link Field} supported by this source task.
     */
    protected abstract Iterable<Field> getAllConfigurationFields();

    /**
     * Loads the connector's persistent offset (if present) via the given loader.
     */
    protected OffsetContext getPreviousOffset(OffsetContext.Loader loader) {
        Map<String, ?> partition = loader.getPartition();

        Map<String, Object> previousOffset = context.offsetStorageReader()
                .offsets(Collections.singleton(partition))
                .get(partition);

        if (previousOffset != null) {
            OffsetContext offsetContext = loader.load(previousOffset);
            LOGGER.info("Found previous offset {}", offsetContext);
            return offsetContext;
        }
        else {
            return null;
        }
    }
}
