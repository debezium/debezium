/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.docker;

import java.time.Duration;

import org.testcontainers.containers.GenericContainer;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.WaitConditions;

public class ZookeeperContainer extends GenericContainer<ZookeeperContainer> {

    public static final String ZOOKEEPER_COMMAND = "zookeeper";
    public static final int ZOOKEEPER_PORT_CLIENT = 2181;

    public ZookeeperContainer(String containerImageName) {
        super(containerImageName);
        defaultConfig();
    }

    public ZookeeperContainer() {
        this(ConfigProperties.DOCKER_IMAGE_KAFKA_RHEL);
    }

    public String serverAddress() {
        return getNetworkAliases().get(0) + ":" + ZOOKEEPER_PORT_CLIENT;
    }

    private void defaultConfig() {
        withExposedPorts(ZOOKEEPER_PORT_CLIENT);
        withCommand(ZOOKEEPER_COMMAND);
        withStartupTimeout(Duration.ofMinutes(WaitConditions.scaled(1)));
    }

}
