/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.sqlserver;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_SQLSERVER_SA_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DOCKER_IMAGE_SQLSERVER;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;

import io.debezium.testing.system.tools.AbstractDockerDeployer;

/**
 * @author Jakub Cechacek
 */
public final class DockerSqlServerDeployer
        extends AbstractDockerDeployer<DockerSqlServerController, MSSQLServerContainer<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerSqlServerDeployer.class);

    private DockerSqlServerDeployer(MSSQLServerContainer<?> container) {
        super(container);
    }

    @Override
    protected DockerSqlServerController getController(MSSQLServerContainer<?> container) {
        return new DockerSqlServerController(container);
    }

    public static class Builder
            extends DockerBuilder<Builder, MSSQLServerContainer<?>, DockerSqlServerDeployer> {

        public Builder() {
            this(new MSSQLServerContainer<>(DOCKER_IMAGE_SQLSERVER));
        }

        public Builder(MSSQLServerContainer<?> container) {
            super(container);
        }

        @Override
        public DockerSqlServerDeployer build() {
            container
                    .withPassword(DATABASE_SQLSERVER_SA_PASSWORD)
                    .withEnv("MSSQL_AGENT_ENABLED", "true")
                    .withEnv("MSSQL_PID", "Standard");

            return new DockerSqlServerDeployer(container);
        }
    }
}
