/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.postgresql;

import static io.debezium.testing.system.tools.ConfigProperties.DOCKER_IMAGE_POSTGRESQL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.AbstractDockerDeployer;
import io.debezium.testing.system.tools.databases.docker.DBZPostgreSQLContainer;

/**
 * @author Jakub Cechacek
 */
public final class DockerPostgreSqlDeployer
        extends AbstractDockerDeployer<DockerPostgreSqlController, DBZPostgreSQLContainer<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerPostgreSqlDeployer.class);

    private DockerPostgreSqlDeployer(DBZPostgreSQLContainer<?> container) {
        super(container);
    }

    @Override
    protected DockerPostgreSqlController getController(DBZPostgreSQLContainer<?> container) {
        return new DockerPostgreSqlController(container);
    }

    public static class Builder
            extends DockerBuilder<Builder, DBZPostgreSQLContainer<?>, DockerPostgreSqlDeployer> {

        public Builder() {
            this(new DBZPostgreSQLContainer<>(DOCKER_IMAGE_POSTGRESQL));
        }

        public Builder(DBZPostgreSQLContainer<?> container) {
            super(container);
        }

        @Override
        public DockerPostgreSqlDeployer build() {
            return new DockerPostgreSqlDeployer(container);
        }
    }
}
