/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import java.io.Closeable;

import io.debezium.common.annotation.Incubating;
import io.debezium.service.Service;

/**
 * Registry of Debezium Services.
 *
 * @author Chris Cranford
 */
@Incubating
public interface ServiceRegistry extends Closeable {
    /**
     * Get a service by class type.
     *
     * @param serviceClass the service class
     * @return the requested service or {@code null} if the service was not found
     * @param <T> the service class type
     */
    <T extends Service> T getService(Class<T> serviceClass);

    /**
     * Register a service provider with the service registry. A provider allows the construction
     * and resolution of services lazily upon first request and use.
     *
     * @param serviceProvider the service provider, should not be {@code null}
     * @param <T> the service type
     */
    <T extends Service> void registerServiceProvider(ServiceProvider<T> serviceProvider);

    /**
     * Closes the service registry.
     */
    @Override
    void close();
}
