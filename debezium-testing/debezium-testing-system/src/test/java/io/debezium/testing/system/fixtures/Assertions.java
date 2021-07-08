/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures;

import io.debezium.testing.system.assertions.AvroKafkaAssertions;
import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.assertions.PlainKafkaAssertions;
import io.debezium.testing.system.fixtures.kafka.KafkaRuntimeFixture;
import io.debezium.testing.system.fixtures.registry.RegistryRuntimeFixture;

public interface Assertions
        extends RegistryRuntimeFixture, KafkaRuntimeFixture {

    default KafkaAssertions<?, ?> assertions() {
        if (getRegistryController().isPresent()) {
            return new AvroKafkaAssertions(getKafkaController().getDefaultConsumerProperties());
        }
        else {
            return new PlainKafkaAssertions(getKafkaController().getDefaultConsumerProperties());
        }
    }
}
