/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

public class DefaultDatasetNamespaceResolverFactory implements DatasetNamespaceResolverFactory {

    private static final String MONGODB = "mongodb";
    private static final String POSTGRESQL = "postgresql";

    public InputDatasetNamespaceResolver createInput(String connectorName) {
        return switch (connectorName) {
            case MONGODB -> new MongoDbDatasetNamespaceResolver();
            case POSTGRESQL -> new PostgresDatasetNamespaceResolver();
            default -> new DefaultInputDatasetNamespaceResolver();
        };
    }

    @Override
    public OutputDatasetNamespaceResolver createOutput(String connectorName) {
        return new DefaultOutputDatasetNamespaceResolver();
    }
}
