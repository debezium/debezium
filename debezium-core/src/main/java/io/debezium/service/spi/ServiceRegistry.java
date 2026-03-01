/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import java.io.Closeable;

import io.debezium.common.annotation.Incubating;
import io.debezium.service.Service;
import io.debezium.service.UnknownServiceException;

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
     * @return the requested service
     * @param <T> the service class type
     * @throws UnknownServiceException if the requested service is not found
     */
    <T extends Service> T getService(Class<T> serviceClass);

    /**
     * Safely get a service if it exists, or null if it does not.
     *
     * @param serviceClass the service class
     * @return the requested service or {@code null} if the service was not found
     * @param <T> the service class type
     */
    default <T extends Service> T tryGetService(Class<T> serviceClass) {
        try {
            return getService(serviceClass);
        }
        catch (UnknownServiceException e) {
            // ignored
        }
        return null;
    }

    /**
     * Closes the service registry.
     */
    @Override
    void close();
}
