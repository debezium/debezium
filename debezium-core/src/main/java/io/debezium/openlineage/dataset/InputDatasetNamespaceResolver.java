/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import io.debezium.config.Configuration;

public interface InputDatasetNamespaceResolver {

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
    String INPUT_DATASET_NAMESPACE_FORMAT = "%s://%s:%s";

    String resolve(Configuration configuration, String connectorName);
}
