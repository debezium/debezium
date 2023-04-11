/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mysql;

import static io.debezium.testing.system.tools.ConfigProperties.DOCKER_IMAGE_MYSQL_MASTER;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.AbstractDockerDeployer;
import io.debezium.testing.system.tools.databases.docker.DBZMySQLContainer;

/**
 * @author Jakub Cechacek
 */
public final class DockerMySqlDeployer
        extends AbstractDockerDeployer<DockerMysqlController, DBZMySQLContainer<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerMySqlDeployer.class);

    private DockerMySqlDeployer(DBZMySQLContainer<?> container) {
        super(container);
    }

    @Override
    protected DockerMysqlController getController(DBZMySQLContainer<?> container) {
        return new DockerMysqlController(container);
    }

    public static class Builder
            extends DockerBuilder<Builder, DBZMySQLContainer<?>, DockerMySqlDeployer> {

        public Builder() {
            this(new DBZMySQLContainer<>(DOCKER_IMAGE_MYSQL_MASTER));
        }

        public Builder(DBZMySQLContainer<?> container) {
            super(container);
        }

        @Override
        public DockerMySqlDeployer build() {
            return new DockerMySqlDeployer(container);
        }
    }
}
