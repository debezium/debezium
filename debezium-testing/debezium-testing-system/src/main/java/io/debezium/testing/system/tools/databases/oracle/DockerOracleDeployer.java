/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.oracle;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_ORACLE_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_ORACLE_PDBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_ORACLE_USERNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DOCKER_IMAGE_ORACLE;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.system.tools.AbstractDockerDeployer;

/**
 * @author Jakub Cechacek
 */
public final class DockerOracleDeployer
        extends AbstractDockerDeployer<DockerOracleController, OracleContainer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerOracleDeployer.class);

    private DockerOracleDeployer(OracleContainer container) {
        super(container);
    }

    @Override
    protected DockerOracleController getController(OracleContainer container) {
        return new DockerOracleController(container);
    }

    public static class Builder
            extends DockerBuilder<Builder, OracleContainer, DockerOracleDeployer> {

        public Builder() {
            this(new OracleContainer(
                    DockerImageName.parse(DOCKER_IMAGE_ORACLE).asCompatibleSubstituteFor("gvenzl/oracle-xe")));
        }

        public Builder(OracleContainer container) {
            super(container);
        }

        @Override
        public DockerOracleDeployer build() {
            container
                    .withDatabaseName(DATABASE_ORACLE_PDBNAME)
                    .withUsername(DATABASE_ORACLE_USERNAME)
                    .withPassword(DATABASE_ORACLE_PASSWORD)
                    .withStartupTimeout(Duration.of(15, ChronoUnit.MINUTES));

            return new DockerOracleDeployer(container);
        }
    }
}
