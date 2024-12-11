/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import java.util.Map;

import io.debezium.config.Configuration;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;

/**
 * Factory class for creating instances of {@link ColumnNamingStrategy}
 *
 * @author Gustavo Lira
 */
public class ColumnNamingStrategyFactory {

    /**
     * Creates an instance of {@link ColumnNamingStrategy} based on the configuration.
     *
     * @param config the main configuration object
     * @param props  additional properties for configuring the naming strategy
     * @return an instance of {@link ColumnNamingStrategy}
     */
    public static ColumnNamingStrategy createColumnNamingStrategy(Configuration config, Map<String, String> props) {
        String strategyClassName = config.getString(JdbcSinkConnectorConfig.COLUMN_NAMING_STRATEGY_FIELD);

        if (CustomColumnNamingStrategy.class.getName().equals(strategyClassName)) {
            CustomColumnNamingStrategy strategy = new CustomColumnNamingStrategy();
            strategy.configure(props);
            return strategy;
        }

        // Use the default method to create an instance of the specified class
        return config.getInstance(JdbcSinkConnectorConfig.COLUMN_NAMING_STRATEGY_FIELD, ColumnNamingStrategy.class);
    }
}
