/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import io.debezium.config.Configuration;

public class MongoDbDatasetNamespaceResolver implements InputDatasetNamespaceResolver {

    private static final String MONGODB_CONNECTION_STRING_PROPERTY = "mongodb.connection.string";

    @Override
    public String resolve(Configuration configuration, String connectorName) {
        return removeConnectionStringOptions(configuration.getString(MONGODB_CONNECTION_STRING_PROPERTY));
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
