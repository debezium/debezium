/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.docker;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import com.github.dockerjava.api.command.InspectContainerResponse;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.WaitConditions;

public class KafkaContainer extends GenericContainer<KafkaContainer> {

    public static final String KAFKA_COMMAND = "kafka";
    public static final int KAFKA_CONTROLLER_PORT = 9093;
    public static final int KAFKA_BROKER_PORT = 9092;

    private static final AtomicInteger COUNTER = new AtomicInteger();

    public KafkaContainer(String containerImageName) {
        super(containerImageName);
        defaultConfig();
    }

    public KafkaContainer() {
        this(ConfigProperties.DOCKER_IMAGE_KAFKA_RHEL);
    }

    private void defaultConfig() {
        addFixedExposedPort(KAFKA_BROKER_PORT, KAFKA_BROKER_PORT);
        addFixedExposedPort(KAFKA_CONTROLLER_PORT, KAFKA_CONTROLLER_PORT);
        withCommand(KAFKA_COMMAND);
        withEnv("KAFKA_LISTENERS", String.format("CONTROLLER://0.0.0.0:%d,BROKER://0.0.0.0:%d", KAFKA_CONTROLLER_PORT, KAFKA_BROKER_PORT));
        withEnv("KAFKA_ADVERTISED_LISTENERS", String.format("CONTROLLER://%s:%d,BROKER://%s:%d", getHost(), KAFKA_CONTROLLER_PORT, getHost(), KAFKA_BROKER_PORT));
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT");
        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");
        withStartupTimeout(Duration.ofMinutes(WaitConditions.scaled(3)));
    }

    public KafkaContainer withZookeeper(ZookeeperContainer zookeeper) {
        return this
                .dependsOn(zookeeper)
                .withZookeeper(zookeeper.getNetwork(), zookeeper.serverAddress());
    }

    public KafkaContainer withZookeeper(Network network, String zookeeperServers) {
        return this
                .withNetwork(network)
                .withEnv("ZOOKEEPER_CONNECT", zookeeperServers);
    }

    public String getPublicBootstrapAddress() {
        return getHost() + ":" + KAFKA_BROKER_PORT;
    }

    public String getBootstrapAddress() {
        return getNetworkAliases().getFirst() + ":" + KAFKA_BROKER_PORT;
    }

    @Override
    protected void containerIsStopped(InspectContainerResponse containerInfo) {
        super.containerIsStopped(containerInfo);
        COUNTER.decrementAndGet();
    }
}
