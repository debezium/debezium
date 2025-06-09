/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import io.openlineage.client.OpenLineageClient;

public record ProcessingEngineMetadata(String version, String name,
        String openlineageAdapterVersion) {

    public static final String DEBEZIUM = "Debezium";

    public static ProcessingEngineMetadata debezium(ConnectorContext connectorContext) {
        return new ProcessingEngineMetadata(connectorContext.version(), DEBEZIUM, getPackageVersion(OpenLineageClient.class));
    }

    private static String getPackageVersion(Class<?> clazz) {
        Package pkg = clazz.getPackage();
        if (pkg != null) {
            String version = pkg.getImplementationVersion();
            return version != null ? version : "N/A";
        }
        return "N/A";
    }
}
