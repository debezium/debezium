/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import java.util.Map;

/**
 * Test {@link DatasetNamespaceResolverProvider} registered via {@code META-INF/services} for unit tests.
 */
public class TestDatasetNamespaceResolverProvider implements DatasetNamespaceResolverProvider {

    public static final String CONNECTOR_NAME = "test-sink-connector";
    public static final String RESOLVED_NAMESPACE = "test-sink://namespace";

    @Override
    public boolean supports(String connectorName) {
        return CONNECTOR_NAME.equals(connectorName);
    }

    @Override
    public DatasetNamespaceResolver create() {
        return (Map<String, String> configuration, String connectorName) -> RESOLVED_NAMESPACE;
    }
}
