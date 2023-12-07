/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.processors.spi.ConnectionAware;
import io.debezium.processors.spi.ConnectorConfigurationAware;
import io.debezium.processors.spi.PostProcessor;
import io.debezium.processors.spi.RelationalDatabaseSchemaAware;
import io.debezium.processors.spi.ValueConverterAware;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.ValueConverterProvider;

/**
 * Registry of all post processors that are provided by the connector configuration.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class PostProcessorRegistry implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessorRegistry.class);

    @Immutable
    private final List<PostProcessor> processors;

    public PostProcessorRegistry(List<PostProcessor> processors) {
        if (processors == null) {
            this.processors = Collections.emptyList();
        }
        else {
            this.processors = Collections.unmodifiableList(processors);
        }
        LOGGER.info("Registered {} post processors.", this.processors.size());
    }

    @Override
    public void close() {
        for (PostProcessor processor : processors) {
            processor.close();
        }
    }

    public List<PostProcessor> getProcessors() {
        return this.processors;
    }

    /**
     * Get the injection builder for applying dependency injection to post-processors.
     *
     * @return the injection builder
     * @param <T> the connection type implementation
     */
    public <T> InjectionBuilder<T> injectionBuilder() {
        return new InjectionBuilder<>(this);
    }

    /**
     * Builder contract for injecting dependencies.
     *
     * @param <T> the connection type
     */
    public static class InjectionBuilder<T> {
        private final PostProcessorRegistry registry;

        private ValueConverterProvider valueConverterProvider;
        private T connection;
        private RelationalDatabaseSchema relationalSchema;
        private CommonConnectorConfig connectorConfig;

        InjectionBuilder(PostProcessorRegistry registry) {
            this.registry = registry;
        }

        public InjectionBuilder<T> withValueConverter(ValueConverterProvider valueConverterProvider) {
            this.valueConverterProvider = valueConverterProvider;
            return this;
        }

        public InjectionBuilder<T> withConnection(T connection) {
            this.connection = connection;
            return this;
        }

        public InjectionBuilder<T> withRelationalSchema(RelationalDatabaseSchema schema) {
            this.relationalSchema = schema;
            return this;
        }

        public InjectionBuilder<T> withConnectorConfig(CommonConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
            return this;
        }

        @SuppressWarnings("unchecked")
        public void apply() {
            for (PostProcessor processor : registry.getProcessors()) {
                if (connection != null && processor instanceof ConnectionAware) {
                    ((ConnectionAware<T>) processor).setDatabaseConnection(connection);
                }
                if (valueConverterProvider != null && processor instanceof ValueConverterAware) {
                    ((ValueConverterAware) processor).setValueConverter(valueConverterProvider);
                }
                if (relationalSchema != null && processor instanceof RelationalDatabaseSchemaAware) {
                    ((RelationalDatabaseSchemaAware) processor).setDatabaseSchema(relationalSchema);
                }
                if (connectorConfig != null && processor instanceof ConnectorConfigurationAware) {
                    ((ConnectorConfigurationAware) processor).setConnectorConfig(connectorConfig);
                }
            }
        }
    }
}
