/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import java.util.stream.Stream;

import org.apache.maven.artifact.versioning.ComparableVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import io.debezium.testing.system.tools.AbstractDockerDeployer;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.Deployer;
import io.debezium.testing.system.tools.kafka.docker.KafkaContainer;
import io.debezium.testing.system.tools.kafka.docker.ZookeeperContainer;

public class DockerKafkaDeployer
        extends AbstractDockerDeployer<DockerKafkaController, KafkaContainer>
        implements Deployer<DockerKafkaController> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerKafkaDeployer.class);

    public DockerKafkaDeployer(KafkaContainer container) {
        super(container);
    }

    @Override
    protected DockerKafkaController getController(KafkaContainer container) {
        return new DockerKafkaController(container);
    }

    @Override
    public DockerKafkaController deploy() {
        DockerKafkaController controller;

        if (!shouldKRaftBeUsed()) {
            ZookeeperContainer zookeeperContainer = new ZookeeperContainer()
                    .withNetwork(container.getNetwork());
            container.withZookeeper(zookeeperContainer);
            Startables.deepStart(Stream.of(zookeeperContainer, container)).join();

            controller = getController(container);
            controller.setZookeeperContainer(zookeeperContainer);
            LOGGER.info("Using Kafka in Zookeeper mode");
        }
        else {
            controller = getController(container);
            LOGGER.info("Using Kafka in KRaft mode");
        }

        return controller;
    }

    private boolean shouldKRaftBeUsed() {
        ComparableVersion kafkaVersion = new ComparableVersion(ConfigProperties.VERSION_KAFKA);
        return ConfigProperties.FORCE_KRAFT || kafkaVersion.compareTo(new ComparableVersion("4.0.0")) >= 0;
    }

    public static class Builder
            extends AbstractDockerDeployer.DockerBuilder<Builder, KafkaContainer, DockerKafkaDeployer> {

        public Builder() {
            this(new KafkaContainer());
        }

        public Builder(KafkaContainer container) {
            super(container);
        }

        @Override
        public DockerKafkaDeployer build() {
            return new DockerKafkaDeployer(container);
        }
    }
}
