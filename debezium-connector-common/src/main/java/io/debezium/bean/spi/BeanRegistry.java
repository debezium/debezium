/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bean.spi;

import java.util.Map;
import java.util.Set;

import io.debezium.common.annotation.Incubating;
import io.debezium.service.Service;

/**
 * Represents a bean registry used to lookup components by name and type.
 *
 * The {@link BeanRegistry} extends the {@link Service} contract, allowing it to be exposed
 * via the service registry to any service easily.
 *
 * @author Chris Cranford
 */
@Incubating
public interface BeanRegistry extends Service {
    /**
     * Finds all beans that are registered by the specified type.
     *
     * @param type the class type to lookup
     * @return set of all beans found, may be empty, never {@code null}
     */
    <T> Set<T> findByType(Class<T> type);

    /**
     * Finds all beans that are registered with the specified type.
     *
     * @param type the class type to lookup
     * @return map of bean instances with their mapping register names, may be empty, never {@code null}
     */
    <T> Map<String, T> findByTypeWithName(Class<T> type);

    /**
     * Lookup a specific bean by its name and type.
     *
     * @param name the bean name to find
     * @param type the bean type to find
     * @return the bean or {@code null} if the bean could not be found
     */
    <T> T lookupByName(String name, Class<T> type);

    /**
     * Adds a bean to the registry.
     *
     * @param name the bean name the instance should be registered with, should not be {@code null}
     * @param type the bean class type, should not be {@code null}
     * @param bean the bean instance, should not be {@code null}
     */
    void add(String name, Class<?> type, Object bean);

    /**
     * Adds a bean to the registry, resolving the class type from the {@code bean} instance.
     *
     * @param name the bean name the instance should be registered with, should not be {@code null}
     * @param bean the bean instance, should not be {@code null}
     */
    void add(String name, Object bean);

    /**
     * Remove a bean from the registry by name.
     *
     * @param name the bean name
     */
    void remove(String name);
}
