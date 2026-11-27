/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.kafka.common.config.ConfigException;

import io.debezium.ai.decision.spi.DecisionServiceFactory;
import io.debezium.config.Field;

/**
 * Discovery of the {@link DecisionServiceFactory} implementations.
 *
 * @author Jiri Pechanec
 */
final class DecisionServiceFactories {

    private DecisionServiceFactories() {
    }

    static List<DecisionServiceFactory> loadAll() {
        final List<DecisionServiceFactory> factories = new ArrayList<>();
        ServiceLoader.load(DecisionServiceFactory.class).forEach(factories::add);
        return factories;
    }

    /**
     * @return union of the options of all the discovered providers
     */
    static List<Field> allConfigFields() {
        final List<Field> fields = new ArrayList<>();
        for (final var factory : loadAll()) {
            factory.getConfigFields().forEach(fields::add);
        }
        return fields;
    }

    static DecisionServiceFactory find(final String name) {
        final var factories = loadAll();
        for (final var factory : factories) {
            if (factory.name().equalsIgnoreCase(name)) {
                return factory;
            }
        }
        final var names = factories.stream().map(DecisionServiceFactory::name).sorted().toList();
        throw new ConfigException("Unknown decision provider '" + name + "', the available providers are " + names);
    }
}
