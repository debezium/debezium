/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.fixtures;

import io.debezium.testing.openshift.assertions.AvroKafkaAssertions;
import io.debezium.testing.openshift.assertions.KafkaAssertions;
import io.debezium.testing.openshift.assertions.PlainKafkaAssertions;
import io.debezium.testing.openshift.fixtures.kafka.KafkaRuntimeFixture;
import io.debezium.testing.openshift.fixtures.registry.RegistryRuntimeFixture;

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
