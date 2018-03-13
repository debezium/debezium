/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.internal.converters;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import io.debezium.reactive.spi.AsType;
import io.debezium.reactive.spi.Converter;

/**
 * A registry holding references to all converters available on classpath
 *
 * @author Jiri Pechanec
 *
 */
public class ConverterRegistry {

    private final Map<Class<? extends AsType<?>>, Converter<?>> registry = new HashMap<>();

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ConverterRegistry(Map<String, ?> configs) {
        final ServiceLoader<Converter> serviceLoader = ServiceLoader.load(Converter.class);
        for (final Converter c: serviceLoader) {
            c.configure(configs);
            final Class<AsType<?>> asType = c.getConvertedType();
            if(registry.containsKey(asType)) {
                throw new IllegalStateException(
                        "Multiple converters "
                                + c.getClass().getName() + ", "
                                + registry.get(asType).getClass().getName()
                                + " found for the target type " + asType.getName());
            }
            registry.put(asType, c);
        }
    }

    public Converter<?> getConverter(Class<? extends AsType<?>> asType) {
        return registry.computeIfAbsent(asType, x -> {
            throw new IllegalStateException("Converter for " + asType.getName() + " was not found");
        });
    }
}
