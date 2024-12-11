/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.sink.naming.CollectionNamingStrategy;
import io.debezium.sink.naming.DefaultCollectionNamingStrategy;

/**
 * Factory for creating instances of CollectionNamingStrategy.
 *
 * @author Gustavo Lira
 */
public class CollectionNamingStrategyFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionNamingStrategyFactory.class);

    /**
     * Factory method to create an instance of {@link CollectionNamingStrategy} based on the provided configuration.
     * <p>
     * This method determines which implementation of {@link CollectionNamingStrategy} to instantiate.
     * By default, it creates an instance of {@link DefaultCollectionNamingStrategy} if no custom strategy
     * is specified in the connector configuration. For custom strategies, the class name must be provided
     * in the {@code collection.naming.strategy} field of the configuration.
     * </p>
     * <p>
     * If the specified strategy class implements additional configuration, the method ensures that
     * the {@link CollectionNamingStrategy#configure(Map)} method is called with the provided properties.
     * </p>
     *
     * @param config the connector configuration; must not be {@code null}.
     *               This configuration contains the {@code collection.naming.strategy} field, which specifies the strategy to use.
     * @param props  additional properties to be passed to the strategy's {@link CollectionNamingStrategy#configure(Map)} method.
     * @return an instance of the specified {@link CollectionNamingStrategy}.
     * @throws IllegalArgumentException if the specified strategy cannot be instantiated or fails to configure.
     */
    public static CollectionNamingStrategy createCollectionNamingStrategy(Configuration config, Map<String, String> props) {
        String strategyClassName = config.getString(JdbcSinkConnectorConfig.COLLECTION_NAMING_STRATEGY_FIELD);
        LOGGER.debug("Configured CollectionNamingStrategy: {}", strategyClassName);

        if (strategyClassName == null) {
            return configureDefaultStrategy(props);
        }

        if ("table.naming.strategy".equals(strategyClassName)) {
            LOGGER.warn("The 'table.naming.strategy' configuration is deprecated. Use 'collection.naming.strategy' instead.");
            return configureDefaultStrategy(props);
        }

        try {
            if (DefaultCollectionNamingStrategy.class.getName().equals(strategyClassName)) {
                return configureDefaultStrategy(props);
            }

            CollectionNamingStrategy strategy = config.getInstance(
                    JdbcSinkConnectorConfig.COLLECTION_NAMING_STRATEGY_FIELD,
                    CollectionNamingStrategy.class);
            strategy.configure(props);
            return strategy;
        }
        catch (Exception e) {
            LOGGER.error("Failed to create CollectionNamingStrategy for class '{}': {}", strategyClassName, e.getMessage(), e);
            throw new IllegalArgumentException("Failed to create CollectionNamingStrategy for class: " + strategyClassName, e);
        }
    }

    /**
     * Configures and returns an instance of the {@link DefaultCollectionNamingStrategy}.
     *
     * @param props additional properties to configure the strategy.
     * @return a configured instance of {@link DefaultCollectionNamingStrategy}.
     */
    private static CollectionNamingStrategy configureDefaultStrategy(Map<String, String> props) {
        DefaultCollectionNamingStrategy defaultStrategy = new DefaultCollectionNamingStrategy();
        defaultStrategy.configure(props);
        return defaultStrategy;
    }
}
