/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import io.debezium.service.spi.ServiceProvider;

/**
 * Describes a registration for a specific service.
 *
 * @author Chris Cranford
 */
public final class ServiceRegistration<T extends Service> {

    private final Class<T> serviceClass;
    private final ServiceProvider<T> serviceProvider;
    private volatile T service;

    /**
     * Create a service registration for an already existing service instance.
     *
     * @param serviceClass the service class
     * @param service the service instance
     */
    public ServiceRegistration(Class<T> serviceClass, T service) {
        this.serviceClass = serviceClass;
        this.service = service;
        this.serviceProvider = null;
    }

    /**
     * Create a service registration where the service will be initialized on first-use.
     *
     * @param serviceProvider the service provider
     */
    public ServiceRegistration(ServiceProvider<T> serviceProvider) {
        this.serviceClass = serviceProvider.getServiceClass();
        this.serviceProvider = serviceProvider;
    }

    /**
     * Get the service class type
     *
     * @return the class type, never {@code null}
     */
    public Class<T> getServiceClass() {
        return serviceClass;
    }

    /**
     * Get the service provider
     *
     * @return the service provider, may be {@code null}
     */
    public ServiceProvider<T> getServiceProvider() {
        return serviceProvider;
    }

    /**
     * Get the service instance.
     *
     * @return the service instance, may be {@code null} if initialized
     */
    public T getService() {
        return service;
    }

    /**
     * Set the service once initialized, used by {@link ServiceProvider} registrations.
     *
     * @param service the service instance, should not be {@code null}
     */
    public void setService(T service) {
        this.service = service;
    }

}
