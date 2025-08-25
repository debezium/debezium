/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.time.Duration;
import java.util.List;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class DebeziumKafkaContainer extends GenericContainer<DebeziumKafkaContainer> {

    private static final String DEFAULT_IMAGE = "quay.io/debezium/kafka";
    private static final String DEFAULT_TAG = "latest";
    private static final int KAFKA_BROKER_PORT = 9092;
    private static final int KAFKA_CONTROLLER_PORT = 9093;

    public static DebeziumKafkaContainer defaultContainer(Network network) {
        try (DebeziumKafkaContainer kafka = new DebeziumKafkaContainer().withNetwork(network)) {
            return kafka;
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot create KRaftContainer with default image.", e);
        }
    }

    public DebeziumKafkaContainer() {
        this(DockerImageName.parse(DEFAULT_IMAGE).withTag(DEFAULT_TAG));
    }

    private DebeziumKafkaContainer(DockerImageName dockerImage) {
        super(dockerImage);
        defaultconfig();
    }

    private void defaultconfig() {
        addFixedExposedPort(KAFKA_BROKER_PORT, KAFKA_BROKER_PORT);
        addFixedExposedPort(KAFKA_CONTROLLER_PORT, KAFKA_CONTROLLER_PORT);
        withEnv("KAFKA_LISTENERS", String.format("BROKER://0.0.0.0:%d,CONTROLLER://0.0.0.0:%d", KAFKA_BROKER_PORT, KAFKA_CONTROLLER_PORT));
        withEnv("KAFKA_ADVERTISED_LISTENERS", String.format("BROKER://%s,CONTROLLER://%s", getPublicBootstrapAddress(), getBootstrapAddress()));
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT");
        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");
        withStartupTimeout(Duration.ofMinutes(3));
    }

    public DebeziumKafkaContainer withNetworkAlias(String networkAlias) {
        List<String> networkAliasList = getNetworkAliases();
        networkAliasList.add(networkAlias);
        setNetworkAliases(networkAliasList);
        return this;
    }

    /**
     * The Bootstrap address returned by this method must be reachable form arbitrary network.
     * @return Publicly reachable Kafka Bootstrap Server address
     */
    public String getPublicBootstrapAddress() {
        return String.format("%s:%s", getHost(), KAFKA_BROKER_PORT);
    }

    /**
     * The Bootstrap address returned by this method may not be reachable form arbitrary network.
     * @return Kafka Bootstrap Server address
     */
    public String getBootstrapAddress() {
        return String.format("%s:%s", getHost(), KAFKA_CONTROLLER_PORT);
    }
}