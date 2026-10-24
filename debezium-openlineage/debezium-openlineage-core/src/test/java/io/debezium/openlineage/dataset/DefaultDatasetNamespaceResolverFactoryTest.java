/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import io.debezium.openlineage.dataset.DatasetMetadata.DataStore;

class DefaultDatasetNamespaceResolverFactoryTest {

    private static final DefaultDatasetNamespaceResolverFactory FACTORY = new DefaultDatasetNamespaceResolverFactory();

    @Test
    void shouldCreateSinkProvidedResolverFromServiceLoader() {
        DatasetNamespaceResolver resolver = FACTORY.create(DataStore.DATABASE, TestDatasetNamespaceResolverProvider.CONNECTOR_NAME);
        assertSame(TestDatasetNamespaceResolverProvider.RESOLVED_NAMESPACE,
                resolver.resolve(Collections.emptyMap(), TestDatasetNamespaceResolverProvider.CONNECTOR_NAME));
    }
}
