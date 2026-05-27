/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;

/**
 * An implementation of the {@link ServiceProvider} contract for the {@link QueueProviderService}.
 *
 * @author Chris Cranford
 */
public class QueueProviderServiceProvider implements ServiceProvider<QueueProviderService> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueProviderServiceProvider.class);

    @Override
    public QueueProviderService createService(Configuration configuration, ServiceRegistry serviceRegistry) {
        final BeanRegistry beanRegistry = serviceRegistry.tryGetService(BeanRegistry.class);
        final CommonConnectorConfig connectorConfig = beanRegistry.lookupByName(
                StandardBeanNames.CONNECTOR_CONFIG, CommonConnectorConfig.class);

        final String queueProviderType = connectorConfig.getQueueProviderType();

        @SuppressWarnings("rawtypes")
        List<QueueProvider> providers = StreamSupport.stream(
                ServiceLoader.load(QueueProvider.class).spliterator(), false)
                .filter(p -> p.name().equalsIgnoreCase(queueProviderType))
                .toList();

        if (providers.isEmpty()) {
            throw new DebeziumException(
                    "Unable to find '%s' queue provider. Please check your configuration.".formatted(queueProviderType));
        }

        if (providers.size() > 1) {
            throw new DebeziumException(
                    "Found multiple implementations for '%s' queue provider. Please verify your configuration.".formatted(queueProviderType));
        }

        @SuppressWarnings("rawtypes")
        final QueueProvider queueProvider = providers.get(0);
        LOGGER.info("Using QueueProvider {}", queueProvider.getClass().getName());
        queueProvider.configure(configuration.asMap());

        return new QueueProviderService(queueProvider);
    }

    @Override
    public Class<QueueProviderService> getServiceClass() {
        return QueueProviderService.class;
    }
}
