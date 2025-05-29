/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;

public class DefaultInputDatasetNamespaceResolver implements InputDatasetNamespaceResolver {

    @Override
    public String resolve(Configuration configuration, String connectorName) {
        return String.format(INPUT_DATASET_NAMESPACE_FORMAT,
                connectorName,
                configuration.getString(CommonConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME),
                configuration.getString(CommonConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT));
    }
}
