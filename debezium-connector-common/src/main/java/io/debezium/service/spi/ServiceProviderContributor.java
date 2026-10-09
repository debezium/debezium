/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import io.debezium.common.annotation.Incubating;

/**
 * Contract for supplying custom Debezium {@link io.debezium.service.Service services}. Contributors
 * are applied after Debezium's default service providers are registered, so a contributed provider
 * replaces the default provider for the same service.
 *
 * @author Chris Cranford
 */
@Incubating
public interface ServiceProviderContributor {
    /**
     * Contribute a custom service to the {@link ServiceRegistry}.
     *
     * @param registryBuilder the service registry builder, never {@code null}
     */
    void contribute(ServiceRegistryBuilder registryBuilder);
}
