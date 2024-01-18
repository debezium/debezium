/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.kafka.docker.KafkaContainer;

/**
 * This class provides control over Kafka instance deployed as DockerContainer
 *
 * @author Jakub Cechacek
 */
public class DockerKafkaController implements KafkaController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerKafkaController.class);

    private final KafkaContainer container;

    public DockerKafkaController(KafkaContainer container) {
        this.container = container;
    }

    public KafkaContainer getContainer() {
        return container;
    }

    @Override
    public String getPublicBootstrapAddress() {
        return container.getPublicBootstrapAddress();
    }

    @Override
    public String getBootstrapAddress() {
        return container.getBootstrapAddress();
    }

    @Override
    public String getTlsBootstrapAddress() {
        return null;
    }

    @Override
    public boolean undeploy() {
        container.stop();
        return container.isRunning();
    }

    @Override
    public void waitForCluster() {
        await()
                .atMost(scaled(5), MINUTES)
                .until(container::isRunning);
    }
}
