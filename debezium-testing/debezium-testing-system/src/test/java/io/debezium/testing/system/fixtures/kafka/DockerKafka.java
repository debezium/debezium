/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.kafka;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.assertions.PlainKafkaAssertions;
import io.debezium.testing.system.tools.kafka.DockerKafkaConnectController;
import io.debezium.testing.system.tools.kafka.DockerKafkaConnectDeployer;
import io.debezium.testing.system.tools.kafka.DockerKafkaController;
import io.debezium.testing.system.tools.kafka.DockerKafkaDeployer;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import fixture5.TestFixture;
import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { Network.class }, provides = { KafkaController.class, KafkaConnectController.class, KafkaAssertions.class })
public class DockerKafka extends TestFixture {
    private final Network network;
    private DockerKafkaController kafkaController;
    private DockerKafkaConnectController connectController;

    public DockerKafka(@NotNull ExtensionContext.Store store) {
        super(store);
        this.network = retrieve(Network.class);
    }

    @Override
    public void setup() {
        DockerKafkaDeployer kafkaDeployer = new DockerKafkaDeployer.Builder()
                .withNetwork(network)
                .build();
        kafkaController = kafkaDeployer.deploy();

        DockerKafkaConnectDeployer connectDeployer = new DockerKafkaConnectDeployer.Builder()
                .withKafka(kafkaController)
                .build();
        connectController = connectDeployer.deploy();

        store(KafkaController.class, kafkaController);
        store(KafkaConnectController.class, connectController);
        store(KafkaAssertions.class, new PlainKafkaAssertions(kafkaController.getDefaultConsumerProperties()));
    }

    @Override
    public void teardown() {
        if (kafkaController != null) {
            kafkaController.undeploy();
        }
        if (connectController != null) {
            connectController.undeploy();
        }
    }
}
