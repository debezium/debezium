/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import io.debezium.common.annotation.Incubating;
import io.debezium.service.Service;

/**
 * Builder contract for creating a {@link ServiceRegistry} instance.
 *
 * @author Chris Cranford
 */
@Incubating
public interface ServiceRegistryBuilder {
    /**
     * Register a service provider with the service registry. A provider allows the construction
     * and resolution of services lazily as they're needed.
     *
     * @param serviceProvider the service provider, should not be {@code null}
     * @param <T> the service type
     */
    <T extends Service> void registerServiceProvider(ServiceProvider<T> serviceProvider);
}
