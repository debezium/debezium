/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import io.debezium.annotation.JavaServiceLoadable;
import io.debezium.common.annotation.Incubating;

/**
 * Contract for supplying custom Debezium {@link io.debezium.service.Service services}.
 *
 * @author Chris Cranford
 */
@Incubating
@JavaServiceLoadable
public interface ServiceProviderContributor {
    /**
     * Contribute a custom service to the {@link ServiceRegistry}.
     *
     * @param registryBuilder the service registry builder, never {@code null}
     */
    void contribute(ServiceRegistryBuilder registryBuilder);
}
