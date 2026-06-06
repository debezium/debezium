/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.informix;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.system.tools.AbstractDockerDeployer;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.testcontainers.InformixContainer;

public final class DockerInformixDeployer
        extends AbstractDockerDeployer<DockerInformixController, InformixContainer> {

    private DockerInformixDeployer(InformixContainer container) {
        super(container);
    }

    @Override
    protected DockerInformixController getController(InformixContainer container) {
        return new DockerInformixController(container);
    }

    public static class Builder
            extends DockerBuilder<Builder, InformixContainer, DockerInformixDeployer> {

        public Builder() {
            this(new InformixContainer(DockerImageName.parse(ConfigProperties.DOCKER_IMAGE_INFORMIX)));
        }

        public Builder(InformixContainer container) {
            super(container);
        }

        @Override
        public DockerInformixDeployer build() {
            container
                    .withEnv("LICENSE", "accept")
                    .withPrivilegedMode(true)
                    .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES));

            return new DockerInformixDeployer(container);
        }
    }
}
