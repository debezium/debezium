/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

public class DatasetNamespaceResolverFactory {

    public static DatasetNamespaceResolver create(String connectorName) {
        return switch (connectorName) {
            case "mongodb" -> new MongoDbDatasetNamespaceResolver();
            case "postgresql" -> new PostgresDatasetNamespaceResolver();
            default -> new DefaultDatasetNamespaceResolver();
        };
    }
}
