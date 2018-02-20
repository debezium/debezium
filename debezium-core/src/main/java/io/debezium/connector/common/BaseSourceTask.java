/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;

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

        LOGGER.info("Starting " + getClass().getSimpleName() + " with configuration:");
        config.forEach((propName, propValue) -> {
            LOGGER.info("   {} = {}", propName, propValue);
        });

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
}
