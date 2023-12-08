/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import io.debezium.common.annotation.Incubating;

/**
 * Allows the injection of the {@link ServiceRegistry} during service configuration.
 *
 * @author Chris Cranford
 */
@Incubating
public interface ServiceRegistryAware {
    /**
     * Callback to inject the service registry.
     *
     * @param serviceRegistry the service registry
     */
    void injectServiceRegistry(ServiceRegistry serviceRegistry);
}
