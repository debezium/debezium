/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class DebeziumKafkaContainer {
    private static final String defaultImage = "quay.io/debezium/confluentinc-cp-kafka:7.2.10";

    public static KafkaContainer defaultKRaftContainer(Network network) {
        try (
                KafkaContainer kafka = new KafkaContainer(DockerImageName.parse(defaultImage)
                        .asCompatibleSubstituteFor("confluentinc/cp-kafka"))
                        .withNetwork(network)
                        .withKraft()) {
            return kafka;
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot create KRaftContainer with default image.", e);
        }
    }

    public static KafkaContainer defaultKafkaContainer(Network network) {
        try (
                KafkaContainer kafka = new KafkaContainer(DockerImageName.parse(defaultImage))
                        .withNetwork(network)) {
            return kafka;
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot create KafkaContainer with default image.", e);
        }
    }
}
