/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

class DatasetNamespaceResolverProvidersTest {

    @Test
    void shouldLoadResolverFromServiceLoader() {
        DatasetNamespaceResolver resolver = DatasetNamespaceResolverProviders.get(TestDatasetNamespaceResolverProvider.CONNECTOR_NAME);

        assertThat(resolver.resolve(Map.of(), TestDatasetNamespaceResolverProvider.CONNECTOR_NAME))
                .isEqualTo(TestDatasetNamespaceResolverProvider.RESOLVED_NAMESPACE);
    }

    @Test
    void shouldFailWhenNoProviderSupportsConnector() {
        assertThatThrownBy(() -> DatasetNamespaceResolverProviders.get("connector-without-provider"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("connector-without-provider");
    }
}
