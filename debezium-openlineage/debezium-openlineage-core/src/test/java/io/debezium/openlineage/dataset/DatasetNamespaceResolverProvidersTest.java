/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

class DatasetNamespaceResolverProvidersTest {

    @Test
    void shouldLoadResolverFromServiceLoader() {
        DatasetNamespaceResolver resolver = DatasetNamespaceResolverProviders
                .get(TestDatasetNamespaceResolverProvider.CONNECTOR_NAME)
                .map(DatasetNamespaceResolverProvider::create)
                .orElseThrow();

        assertThat(resolver.resolve(Map.of(), TestDatasetNamespaceResolverProvider.CONNECTOR_NAME))
                .isEqualTo(TestDatasetNamespaceResolverProvider.RESOLVED_NAMESPACE);
    }

    @Test
    void shouldReturnEmptyWhenNoProviderSupportsConnector() {
        assertThat(DatasetNamespaceResolverProviders.get("connector-without-provider")).isEmpty();
    }
}
