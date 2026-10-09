/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.service.Service;

/**
 * Resolves the instances of pluggable contracts that Debezium creates from the connector configuration,
 * which allows the runtime environment to supply instances that it manages.
 *
 * @author Chris Cranford
 */
@Incubating
public interface InstanceResolver extends Service {
    /**
     * Resolves the instance to use for the given contract.
     *
     * @param contract the contract the instance implements, never {@code null}
     * @param configKey the configuration property that names the implementation class, never {@code null}
     * @param fallback creates the instance when the environment does not supply one, never {@code null}
     * @param <T> the contract type
     * @return the resolved instance, {@code null} if the fallback is used and creates no instance
     */
    default <T> T resolve(Class<T> contract, String configKey, Supplier<? extends T> fallback) {
        return resolve(contract, configKey, fallback, null);
    }

    /**
     * Resolves the instance to use for the given contract.
     *
     * @param contract the contract the instance implements, never {@code null}
     * @param configKey the configuration property that names the implementation class, never {@code null}
     * @param fallback creates the instance when the environment does not supply one, never {@code null}
     * @param initializer applied only to an instance that the environment supplies, as such an instance
     *            is not given its configuration when it is constructed, may be {@code null}
     * @param <T> the contract type
     * @return the resolved instance, {@code null} if the fallback is used and creates no instance
     */
    <T> T resolve(Class<T> contract, String configKey, Supplier<? extends T> fallback, Consumer<? super T> initializer);

    /**
     * Resolves the instance to use for the given contract, falling back to creating an instance of the
     * class named by the configuration field using its constructor that accepts {@link Properties}.
     *
     * @param config the configuration that names the implementation class, never {@code null}
     * @param field the configuration field that names the implementation class, never {@code null}
     * @param type the contract the instance implements, never {@code null}
     * @param props the properties passed to the constructor of the fallback instance, never {@code null}
     * @param initializer applied only to an instance that the environment supplies, as such an instance
     *            is not given its configuration when it is constructed, may be {@code null}
     * @param <T> the contract type
     * @return the resolved instance, {@code null} if the fallback is used and creates no instance
     * @see Configuration#getInstance(Field, Class, Properties)
     */
    default <T> T getInstance(Configuration config, Field field, Class<T> type, Properties props, Consumer<? super T> initializer) {
        return resolve(type, field.name(), () -> config.getInstance(field, type, props), initializer);
    }

    /**
     * Resolves all the instances to use for the given contract.
     *
     * @param contract the contract the instances implement, never {@code null}
     * @param fallback creates the instances that Debezium provides itself, never {@code null}
     * @param <T> the contract type
     * @return a new list with the instances from the fallback followed by those that the environment
     *         supplies, never {@code null}
     */
    default <T> List<T> resolveAll(Class<T> contract, Supplier<? extends Collection<? extends T>> fallback) {
        return resolveAll(contract, fallback, null);
    }

    /**
     * Resolves all the instances to use for the given contract. Unlike {@link #resolve}, the instances
     * that the environment supplies are added to the instances from the fallback rather than replacing
     * them.
     *
     * @param contract the contract the instances implement, never {@code null}
     * @param fallback creates the instances that Debezium provides itself, never {@code null}
     * @param initializer applied only to the instances that the environment supplies, as such instances
     *            are not given their configuration when they are constructed, may be {@code null}
     * @param <T> the contract type
     * @return a new list with the instances from the fallback followed by those that the environment
     *         supplies, never {@code null}
     */
    <T> List<T> resolveAll(Class<T> contract, Supplier<? extends Collection<? extends T>> fallback, Consumer<? super T> initializer);
}
