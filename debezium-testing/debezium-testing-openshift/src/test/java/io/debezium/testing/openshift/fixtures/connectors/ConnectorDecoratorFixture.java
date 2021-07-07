/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.fixtures.connectors;

import io.debezium.testing.openshift.tools.kafka.ConnectorConfigBuilder;

public interface ConnectorDecoratorFixture {

    default void decorateConnectorConfig(ConnectorConfigBuilder config) {
        // no-op
    }
}
