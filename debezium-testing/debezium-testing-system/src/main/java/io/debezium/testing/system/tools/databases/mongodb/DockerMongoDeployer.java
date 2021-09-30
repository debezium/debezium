/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import static io.debezium.testing.system.tools.ConfigProperties.DOCKER_IMAGE_MONGO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.system.tools.AbstractDockerDeployer;

/**
 * @author Jakub Cechacek
 */
public final class DockerMongoDeployer
        extends AbstractDockerDeployer<DockerMongoController, MongoDBContainer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerMongoDeployer.class);

    private DockerMongoDeployer(MongoDBContainer container) {
        super(container);
    }

    @Override
    protected DockerMongoController getController(MongoDBContainer container) {
        return new DockerMongoController(container);
    }

    public static class Builder
            extends DockerBuilder<Builder, MongoDBContainer, DockerMongoDeployer> {

        public Builder() {
            this(new MongoDBContainer(
                    DockerImageName.parse(DOCKER_IMAGE_MONGO).asCompatibleSubstituteFor("mongo")));
        }

        public Builder(MongoDBContainer container) {
            super(container);
        }

        @Override
        public DockerMongoDeployer build() {
            return new DockerMongoDeployer(container);
        }
    }
}
