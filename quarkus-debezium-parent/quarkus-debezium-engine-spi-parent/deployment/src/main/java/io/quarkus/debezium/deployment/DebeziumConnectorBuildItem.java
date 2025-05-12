/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment;

import io.debezium.runtime.ConnectorProducer;
import io.quarkus.builder.item.MultiBuildItem;

/**
 * BuildItem for Debezium engine creation
 * Combines Engine name with producer
 *
 * Connectors are expected to return this build item to instrument how create the engine.
 */
public final class DebeziumConnectorBuildItem extends MultiBuildItem {

    private final String name;
    private final Class<? extends ConnectorProducer> producer;

    public DebeziumConnectorBuildItem(String name, Class<? extends ConnectorProducer> producer) {
        this.name = name;
        this.producer = producer;
    }

    public String name() {
        return name;
    }

    public Class<? extends ConnectorProducer> producer() {
        return producer;
    }
}
