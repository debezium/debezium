/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bean;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.bean.spi.BeanRegistry;
import io.debezium.common.annotation.Incubating;

/**
 * The default {@link BeanRegistry} that supports looking up beans.
 *
 * @author Chris Cranford
 */
@Incubating
public class DefaultBeanRegistry implements BeanRegistry {

    private final Map<String, Map<Class<?>, Object>> registry = new LinkedHashMap<>();

    @Override
    public <T> Set<T> findByType(Class<T> type) {
        Set<T> results = new LinkedHashSet<>();
        for (Map.Entry<String, Map<Class<?>, Object>> entry : registry.entrySet()) {
            for (Object value : entry.getValue().values()) {
                if (type.isInstance(value)) {
                    results.add(type.cast(value));
                }
            }
        }
        return results;
    }

    @Override
    public <T> Map<String, T> findByTypeWithName(Class<T> type) {
        Map<String, T> results = new LinkedHashMap<>();
        for (Map.Entry<String, Map<Class<?>, Object>> entry : registry.entrySet()) {
            for (Object value : entry.getValue().values()) {
                if (type.isInstance(value)) {
                    results.put(entry.getKey(), type.cast(value));
                }
            }
        }
        return results;
    }

    @Override
    public <T> T lookupByName(String name, Class<T> type) {
        final Map<Class<?>, Object> map = registry.get(name);
        if (map == null) {
            return null;
        }

        final Object value = getMapValueWithFallbackTypeLookup(map, type);
        if (value == null) {
            return null;
        }

        try {
            return type.cast(value);
        }
        catch (Exception e) {
            final String message = String.format("Found bean '%s' with type '%s', expected type '%s'",
                    name, value.getClass().getName(), type);
            throw new NoSuchBeanException(name, message, e);
        }
    }

    @Override
    public void add(String name, Class<?> type, Object bean) {
        Objects.requireNonNull(name, "Cannot add a bean to the registry with no name.");
        Objects.requireNonNull(type, "Cannot add a bean to the registry with no class type.");
        Objects.requireNonNull(bean, "Cannot add a null bean to the registry.");
        registry.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(type, bean);
    }

    @Override
    public void add(String name, Object bean) {
        Objects.requireNonNull(bean, "Cannot add a null bean to the registry.");
        add(name, bean.getClass(), bean);
    }

    @Override
    public void remove(String name) {
        registry.remove(name);
    }

    private <T> Object findMapValueByType(Map<Class<?>, Object> map, Class<T> type) {
        for (Object value : map.values()) {
            if (type.isInstance(value)) {
                return value;
            }
        }
        return null;
    }

    private <T> Object getMapValueWithFallbackTypeLookup(Map<Class<?>, Object> map, Class<T> type) {
        final Object value = map.get(type);
        return value != null ? value : findMapValueByType(map, type);
    }

}
