/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.kafka;

import io.debezium.testing.system.fixtures.DockerNetwork;
import io.debezium.testing.system.tools.kafka.DockerKafkaConnectController;
import io.debezium.testing.system.tools.kafka.DockerKafkaConnectDeployer;
import io.debezium.testing.system.tools.kafka.DockerKafkaController;
import io.debezium.testing.system.tools.kafka.DockerKafkaDeployer;

public interface DockerKafka
        extends KafkaSetupFixture, KafkaRuntimeFixture, DockerNetwork {

    @Override
    default void setupKafka() {
        DockerKafkaDeployer kafkaDeployer = new DockerKafkaDeployer.Builder()
                .withNetwork(getNetwork())
                .build();
        DockerKafkaController kafka = kafkaDeployer.deploy();

        DockerKafkaConnectDeployer connectDeployer = new DockerKafkaConnectDeployer.Builder()
                .withKafka(kafka)
                .build();
        DockerKafkaConnectController connectController = connectDeployer.deploy();

        setKafkaController(kafka);
        setKafkaConnectController(connectController);
    }

    @Override
    default void teardownKafka() {
        getKafkaConnectController().undeploy();
        getKafkaController().undeploy();
    }
}
