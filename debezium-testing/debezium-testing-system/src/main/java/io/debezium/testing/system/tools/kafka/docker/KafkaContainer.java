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
    public static final int KAFKA_CONTROLLER_PORT = 9092;
    public static final int KAFKA_INTERNAL_PORT = 9093;
    public static final int KAFKA_EXTERNAL_PORT = 9094;

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private final int mappedPort;

    public KafkaContainer(String containerImageName) {
        super(containerImageName);
        mappedPort = KAFKA_EXTERNAL_PORT + COUNTER.getAndIncrement();
        defaultConfig();
    }

    public KafkaContainer() {
        this(ConfigProperties.DOCKER_IMAGE_KAFKA_RHEL);
    }

    private void defaultConfig() {
        addFixedExposedPort(mappedPort, KAFKA_EXTERNAL_PORT);
        addExposedPort(mappedPort);
        withCommand(KAFKA_COMMAND);
        withEnv("KAFKA_LISTENERS",
                String.format("CONTROLLER://0.0.0.0:%d,INTERNAL://0.0.0.0:%d,EXTERNAL://0.0.0.0:%d", KAFKA_CONTROLLER_PORT, KAFKA_INTERNAL_PORT, KAFKA_EXTERNAL_PORT));
        withEnv("KAFKA_ADVERTISED_LISTENERS", String.format("INTERNAL://%s,EXTERNAL://%s", getBootstrapAddress(), getPublicBootstrapAddress()));
        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL");
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT");
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
        return getHost() + ":" + mappedPort;
    }

    public String getBootstrapAddress() {
        return getNetworkAliases().getFirst() + ":" + KAFKA_INTERNAL_PORT;
    }

    @Override
    protected void containerIsStopped(InspectContainerResponse containerInfo) {
        super.containerIsStopped(containerInfo);
        COUNTER.decrementAndGet();
    }
}
