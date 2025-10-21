/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;

import io.strimzi.test.container.StrimziKafkaContainer;

/**
 * This class provides control over Kafka instance deployed as DockerContainer
 *
 * @author Jakub Cechacek
 */
public class DockerKafkaController implements KafkaController {

    private final StrimziKafkaContainer kafkaContainer;

    public StrimziKafkaContainer getKafkaContainer() {
        return kafkaContainer;
    }

    public DockerKafkaController(StrimziKafkaContainer container) {
        this.kafkaContainer = container;
    }

    @Override
    public String getPublicBootstrapAddress() {
        return kafkaContainer.getHost() + ":" + kafkaContainer.getMappedPort(StrimziKafkaContainer.KAFKA_PORT);
    }

    @Override
    public String getBootstrapAddress() {
        return kafkaContainer.getBootstrapServers();
    }

    @Override
    public String getTlsBootstrapAddress() {
        return null;
    }

    @Override
    public boolean undeploy() {
        kafkaContainer.stop();
        return !kafkaContainer.isRunning();
    }

    @Override
    public void waitForCluster() {
        await()
                .atMost(scaled(5), MINUTES)
                .until(kafkaContainer::isRunning);
    }
}
