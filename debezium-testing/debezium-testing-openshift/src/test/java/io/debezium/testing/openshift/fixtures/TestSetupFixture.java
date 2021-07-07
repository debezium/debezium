/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.fixtures;

import io.debezium.testing.openshift.fixtures.connectors.ConnectorSetupFixture;
import io.debezium.testing.openshift.fixtures.databases.DatabaseSetupFixture;
import io.debezium.testing.openshift.fixtures.kafka.KafkaSetupFixture;
import io.debezium.testing.openshift.fixtures.registry.RegistrySetupFixture;

public interface TestSetupFixture
        extends KafkaSetupFixture, DatabaseSetupFixture, ConnectorSetupFixture, RegistrySetupFixture {
}
