/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures;

import io.debezium.testing.system.fixtures.connectors.ConnectorSetupFixture;
import io.debezium.testing.system.fixtures.databases.DatabaseSetupFixture;
import io.debezium.testing.system.fixtures.kafka.KafkaSetupFixture;
import io.debezium.testing.system.fixtures.registry.RegistrySetupFixture;

public interface TestSetupFixture
        extends KafkaSetupFixture, DatabaseSetupFixture, ConnectorSetupFixture, RegistrySetupFixture {
}
