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
import io.debezium.jdbc.JdbcConnection;
import io.debezium.processors.spi.ConnectorConfigurationAware;
import io.debezium.processors.spi.JdbcConnectionAware;
import io.debezium.processors.spi.PostProcessor;
import io.debezium.processors.spi.RelationalDatabaseSchemaAware;
import io.debezium.processors.spi.ValueConverterAware;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.ValueConverterProvider;

/**
 * Registry of all post processors that are provided by the connector configuration.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class PostProcessorRegistry implements Closeable {

    private final Logger LOGGER = LoggerFactory.getLogger(PostProcessorRegistry.class);

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
     * Set the optional dependencies needed by a post processor
     *
     * @param valueConverterProvider the value converter
     * @param connection the JDBC connection
     */
    public void injectDependencies(ValueConverterProvider valueConverterProvider, JdbcConnection connection, RelationalDatabaseSchema schema,
                                   RelationalDatabaseConnectorConfig connectorConfig) {
        for (PostProcessor processor : processors) {
            if (processor instanceof JdbcConnectionAware) {
                ((JdbcConnectionAware) processor).setDatabaseConnection(connection);
            }
            if (processor instanceof ValueConverterAware) {
                ((ValueConverterAware) processor).setValueConverter(valueConverterProvider);
            }
            if (processor instanceof RelationalDatabaseSchemaAware) {
                ((RelationalDatabaseSchemaAware) processor).setDatabaseSchema(schema);
            }
            if (processor instanceof ConnectorConfigurationAware) {
                ((ConnectorConfigurationAware) processor).setConnectorConfig(connectorConfig);
            }
        }
    }

}
