/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import io.strimzi.test.container.StrimziKafkaContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.AbstractDockerDeployer;
import io.debezium.testing.system.tools.Deployer;

public class DockerKafkaDeployer
        extends AbstractDockerDeployer<DockerKafkaController, StrimziKafkaContainer>
        implements Deployer<DockerKafkaController> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerKafkaDeployer.class);

    public DockerKafkaDeployer(StrimziKafkaContainer container) {
        super(container);
    }

    @Override
    protected DockerKafkaController getController(StrimziKafkaContainer container) {
        LOGGER.info("Deploying Kafka container");
        return new DockerKafkaController(container);
    }

    @Override
    public DockerKafkaController deploy() {
        return getController(container);
    }

    public static class Builder
            extends AbstractDockerDeployer.DockerBuilder<Builder, StrimziKafkaContainer, DockerKafkaDeployer> {

        public Builder() {
            this(new StrimziKafkaContainer());
        }

        public Builder(StrimziKafkaContainer container) {
            super(container);
        }

        @Override
        public DockerKafkaDeployer build() {
            return new DockerKafkaDeployer(container);
        }
    }
}
