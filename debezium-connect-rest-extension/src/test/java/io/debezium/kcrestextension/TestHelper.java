/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension;

import java.time.Duration;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.debezium.testing.testcontainers.DebeziumContainer;

public class TestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    private static final String KAFKA_HOSTNAME = "kafka-dbz-ui";
    private static final String DEBEZIUM_VERSION = Module.version();

    public static final String API_PREFIX = "/debezium";
    public static final String TRANSFORMS_ENDPOINT = "/transforms";
    public static final String TOPIC_CREATION_ENDPOINT = "/topic-creation";

    private static final Network NETWORK = Network.newNetwork();

    private static final GenericContainer<?> KAFKA_CONTAINER = new GenericContainer<>(
            DockerImageName.parse("quay.io/debezium/kafka:latest").asCompatibleSubstituteFor("kafka"))
            .withNetworkAliases(KAFKA_HOSTNAME)
            .withNetwork(NETWORK)
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@" + KAFKA_HOSTNAME + ":9093")
            .withEnv("CLUSTER_ID", "5Yr1SIgYQz-b-dgRabWx4g")
            .withEnv("NODE_ID", "1");

    private static DebeziumContainer DEBEZIUM_CONTAINER;

    public static DebeziumContainer getDebeziumContainer() {
        return DEBEZIUM_CONTAINER;
    }

    public static void setupDebeziumContainer(String debeziumContainerImageVersion) {
        DEBEZIUM_CONTAINER = new DebeziumContainer(DockerImageName.parse("quay.io/debezium/connect:" + debeziumContainerImageVersion))
                .withEnv("ENABLE_DEBEZIUM_SCRIPTING", "true")
                .withEnv("CONNECT_REST_EXTENSION_CLASSES", "io.debezium.kcrestextension.DebeziumConnectRestExtension")
                .withNetwork(NETWORK)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(
                                "target/debezium-connect-rest-extension-" + DEBEZIUM_VERSION + ".jar"),
                        "/kafka/libs/debezium-kcd-rest-extension-" + DEBEZIUM_VERSION + ".jar")
                .withKafka(KAFKA_CONTAINER.getNetwork(), KAFKA_HOSTNAME + ":9092")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withStartupTimeout(Duration.ofSeconds(90))
                .dependsOn(KAFKA_CONTAINER);
    }

    public static void withEnv(String key, String value) {
        DEBEZIUM_CONTAINER = DEBEZIUM_CONTAINER.withEnv(key, value);
    }

    public static void startContainers() {
        Startables.deepStart(Stream.of(KAFKA_CONTAINER, DEBEZIUM_CONTAINER)).join();
    }

    public static void stopContainers() {
        try {
            if (DEBEZIUM_CONTAINER != null) {
                DEBEZIUM_CONTAINER.stop();
            }
            if (KAFKA_CONTAINER != null) {
                KAFKA_CONTAINER.stop();
            }
        }
        catch (Exception ignored) {
        }
    }
}
