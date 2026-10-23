/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.debezium.service.spi.InstanceResolver;

/**
 * Default implementation of the {@link InstanceResolver}, which always uses the fallback.
 *
 * @author Chris Cranford
 */
public class DefaultInstanceResolver implements InstanceResolver {
    @Override
    public <T> T resolve(Class<T> contract, String configKey, Supplier<? extends T> fallback, Consumer<? super T> initializer) {
        return fallback.get();
    }

    @Override
    public <T> List<T> resolveAll(Class<T> contract, Supplier<? extends Collection<? extends T>> fallback, Consumer<? super T> initializer) {
        return new ArrayList<>(fallback.get());
    }
}
