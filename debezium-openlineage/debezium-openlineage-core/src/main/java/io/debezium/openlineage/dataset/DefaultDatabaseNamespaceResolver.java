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

public class DefaultDatabaseNamespaceResolver implements DatasetNamespaceResolver {

    /**
     * Format string for constructing input dataset namespace identifiers according to the OpenLineage specification.
     * <p>
     * The namespace format follows the pattern "{database}://{host}:{port}" where:
     * <ul>
     * <li>database - the name of the database system (e.g., "postgresql", "mysql", "oracle")</li>
     * <li>host - the hostname or IP address of the database server</li>
     * <li>port - the port number on which the database server is listening</li>
     * </ul>
     * <p>
     * Example usage: {@code String.format(INPUT_DATASET_NAMESPACE_FORMAT, "postgresql", "localhost", "5432")}
     * results in "postgresql://localhost:5432"
     *
     * @see <a href="https://openlineage.io/docs/spec/naming#dataset-naming">OpenLineage Dataset Naming Convention</a>
     */
    public static String INPUT_DATASET_NAMESPACE_FORMAT = "%s://%s:%s";

    @Override
    public String resolve(Map<String, String> configuration, String connectorName) {
        return String.format(INPUT_DATASET_NAMESPACE_FORMAT,
                connectorName,
                configuration.get(DATABASE_CONFIG_PREFIX + DATABASE_HOSTNAME_PROPERTY_NAME),
                configuration.get(DATABASE_CONFIG_PREFIX + DATABASE_PORT_PROPERTY_NAME));
    }
}
