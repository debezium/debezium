/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import java.util.Map;

import io.debezium.config.ConfigurationDefinition;

public class MongoDbDatasetNamespaceResolver implements InputDatasetNamespaceResolver {

    @Override
    public String resolve(Map<String, String> configuration, String connectorName) {
        return removeConnectionStringOptions(configuration.get(ConfigurationDefinition.MONGODB_CONNECTION_STRING_PROPERTY_NAME));
    }

    private String removeConnectionStringOptions(String connectionString) {

        int optionsIndex = connectionString.indexOf('?');
        String result = optionsIndex != -1 ? connectionString.substring(0, optionsIndex) : connectionString;

        if (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }

        return result;
    }
}
