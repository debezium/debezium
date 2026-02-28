/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.service.Service;

/**
 * A contract that defines how a service is provided to the {@link ServiceRegistry}.
 *
 * It is not uncommon for different objects to have various levels of dependency between one
 * another in a system, and the {@link ServiceProvider} allows for the separation of the
 * service registration and the service construction.
 *
 * In essence, the provider is registered with the service registry. Then as services are
 * initialized, they'll request other services that may not have been initialized, and the
 * provider will be called to create such services. This allows the creation of the all
 * services based on their dependency order, and we can in turn tear down the registry in
 * reverse dependency order.
 *
 * @author Chris Cranford
 */
@Incubating
public interface ServiceProvider<T extends Service> {
    /**
     * Get the service class that should be initiated or provided.
     * The service class should be unique in the registry.
     *
     * @return the service class
     */
    Class<T> getServiceClass();

    /**
     * Creates or provides a desired service.
     *
     * @param configuration the connector configuration
     * @param serviceRegistry the service registry
     * @return the constructed service
     */
    T createService(Configuration configuration, ServiceRegistry serviceRegistry);
}
