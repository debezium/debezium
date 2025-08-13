/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import static io.debezium.config.ConfigurationNames.DATABASE_CONFIG_PREFIX;
import static io.debezium.config.ConfigurationNames.DATABASE_HOSTNAME_PROPERTY_NAME;
import static io.debezium.config.ConfigurationNames.DATABASE_PORT_PROPERTY_NAME;

import java.util.Map;

public class PostgresDatasetNamespaceResolver implements InputDatasetNamespaceResolver {

    @Override
    public String resolve(Map<String, String> configuration, String connectorName) {

        return String.format(INPUT_DATASET_NAMESPACE_FORMAT,
                "postgres",
                configuration.get(DATABASE_CONFIG_PREFIX + DATABASE_HOSTNAME_PROPERTY_NAME),
                configuration.get(DATABASE_CONFIG_PREFIX + DATABASE_PORT_PROPERTY_NAME));
    }
}
