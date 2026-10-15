/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import io.debezium.common.annotation.Incubating;
import io.debezium.service.Service;

/**
 * Contract given to a {@link ServiceProviderContributor} for registering service providers while the
 * {@link ServiceRegistry} is being created. It deliberately exposes only registration, not service
 * resolution, because the registry is not yet complete during contribution, and providers registered
 * through it are tracked as contributed so that they take precedence over Debezium's defaults.
 *
 * @author Chris Cranford
 */
@Incubating
public interface ServiceProviderRegistrar {
    /**
     * Register a service provider with the service registry. A provider allows the construction
     * and resolution of services lazily as they're needed.
     *
     * @param serviceProvider the service provider, should not be {@code null}
     * @param <T> the service type
     */
    <T extends Service> void registerServiceProvider(ServiceProvider<T> serviceProvider);
}
