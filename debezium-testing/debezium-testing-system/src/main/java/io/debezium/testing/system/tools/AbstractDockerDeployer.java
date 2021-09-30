/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

public abstract class AbstractDockerDeployer<T, C extends GenericContainer<?>>
        implements Deployer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDockerDeployer.class);

    protected final C container;

    public AbstractDockerDeployer(C container) {
        this.container = container;
    }

    protected abstract T getController(C container);

    protected Logger getCurrentLogger() {
        return LoggerFactory.getLogger(getClass());
    }

    @Override
    public T deploy() {
        Startables.deepStart(Stream.of(container)).join();
        this.container.followOutput(new Slf4jLogConsumer(getCurrentLogger()));
        return getController(container);
    }

    static abstract public class DockerBuilder<B extends DockerBuilder<B, C, D>, C extends GenericContainer<?>, D extends AbstractDockerDeployer<?, C>>
            implements Deployer.Builder<B, D> {

        protected C container;

        public DockerBuilder(C container) {
            Objects.requireNonNull(container, "Missing container");
            this.container = container;
        }

        static protected DockerImageName image(String fullImageName, String substituteFor) {
            return DockerImageName.parse(fullImageName).asCompatibleSubstituteFor(substituteFor);
        }

        public B withNetwork(Network network) {
            container.withNetwork(network);
            return self();
        }

        public B withContainerConfig(Consumer<C> config) {
            config.accept(container);
            return self();
        }
    }
}
