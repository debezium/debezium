/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import io.debezium.common.annotation.Incubating;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;

/**
 * The producer should be implemented by a Debezium Connector extended in Quarkus. It defines:
 * - Configuration mapping between Quarkus, Debezium and the resulting Engine
 * - Configuration validation in the context of multi and single engine
 */
@Incubating
public interface ConnectorProducer {

    /**
     * given a set of {@link DebeziumEngineConfiguration} it creates a {@link DebeziumConnectorRegistry}
     * that contains the related {@link Debezium} engines
     * @param debeziumEngineConfiguration
     * @return the registry that contains the {@link Debezium} engine capturing groups
     */
    DebeziumConnectorRegistry engine(DebeziumEngineConfiguration debeziumEngineConfiguration);
}
