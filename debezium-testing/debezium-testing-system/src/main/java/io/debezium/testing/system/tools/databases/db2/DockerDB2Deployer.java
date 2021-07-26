/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.db2;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_DB2_DBZ_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_DB2_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DOCKER_IMAGE_DB2;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.system.tools.AbstractDockerDeployer;

/**
 * @author Jakub Cechacek
 */
public final class DockerDB2Deployer
        extends AbstractDockerDeployer<DockerDB2Controller, Db2Container> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerDB2Deployer.class);

    private DockerDB2Deployer(Db2Container container) {
        super(container);
    }

    @Override
    protected DockerDB2Controller getController(Db2Container container) {
        return new DockerDB2Controller(container);
    }

    public static class Builder
            extends DockerBuilder<Builder, Db2Container, DockerDB2Deployer> {

        public Builder() {
            this(new Db2Container(
                    DockerImageName.parse(DOCKER_IMAGE_DB2).asCompatibleSubstituteFor("ibmcom/db2")));
        }

        public Builder(Db2Container container) {
            super(container);
        }

        @Override
        public DockerDB2Deployer build() {
            container
                    .withDatabaseName(DATABASE_DB2_DBZ_DBNAME)
                    .withPassword(DATABASE_DB2_PASSWORD)
                    .acceptLicense()
                    .withEnv("ARCHIVE_LOGS", "true")
                    .withEnv("AUTOCONFIG", "true")
                    .withPrivilegedMode(true)
                    .waitingFor(Wait.forLogMessage(".*CDC setup completed.*", 1))
                    .withStartupTimeout(Duration.of(15, ChronoUnit.MINUTES));

            return new DockerDB2Deployer(container);
        }
    }
}
