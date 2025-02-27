/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import java.util.stream.Stream;

import org.testcontainers.lifecycle.Startables;

import io.debezium.testing.system.tools.AbstractDockerDeployer;
import io.debezium.testing.system.tools.Deployer;
import io.debezium.testing.system.tools.kafka.docker.KafkaContainer;
import io.debezium.testing.system.tools.kafka.docker.ZookeeperContainer;

public class DockerKafkaDeployer
        extends AbstractDockerDeployer<DockerKafkaController, KafkaContainer>
        implements Deployer<DockerKafkaController> {

    public DockerKafkaDeployer(KafkaContainer container) {
        super(container);
    }

    @Override
    protected DockerKafkaController getController(KafkaContainer container) {
        return new DockerKafkaController(container);
    }

    @Override
    public DockerKafkaController deploy() {
        ZookeeperContainer zookeeperContainer = new ZookeeperContainer()
                .withNetwork(container.getNetwork());
        container.withZookeeper(zookeeperContainer);

        Startables.deepStart(Stream.of(zookeeperContainer, container)).join();
        DockerKafkaController controller = getController(container);
        controller.setZookeeperContainer(zookeeperContainer);
        return controller;
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
