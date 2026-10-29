/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

/**
 * SPI for providing {@link DatasetNamespaceResolver} implementations for connector-specific datasets.
 * <p>
 * Implementations are discovered via {@link java.util.ServiceLoader} and should be registered in
 * {@code META-INF/services/io.debezium.openlineage.dataset.DatasetNamespaceResolverProvider}.
 * <p>
 */
public interface DatasetNamespaceResolverProvider {

    /**
     * Whether this provider supplies a resolver for the given connector name.
     *
     * @param connectorName the connector name, never null
     * @return true if this provider should handle the connector
     */
    boolean supports(String connectorName);

    /**
     * Creates a namespace resolver instance for a supported connector.
     *
     * @return a resolver instance, never null
     */
    DatasetNamespaceResolver create();
}
