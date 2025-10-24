/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class SchemaRegistryTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryTestResourceLifecycleManager.class);
    private static final String defaultImage = "quay.io/debezium/confluentinc-cp-kafka:7.2.10";

    private static final Network network = Network.newNetwork();
    private static final Integer PORT = 8081;

    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(defaultImage)
            .asCompatibleSubstituteFor("confluentinc/cp-kafka"))
            .withNetwork(network);

    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer()
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .dependsOn(kafkaContainer)
            .withStartupTimeout(Duration.ofSeconds(90));

    @Override
    public Map<String, String> start() {
        Startables.deepStart(Stream.of(kafkaContainer, schemaRegistryContainer)).join();

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.format.schema.registry.url", getSchemaRegistryUrl());

        return params;
    }

    @Override
    public void stop() {
        try {
            if (schemaRegistryContainer != null) {
                schemaRegistryContainer.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    private static String getSchemaRegistryUrl() {
        return "http://" + schemaRegistryContainer.getHost() + ":" + schemaRegistryContainer.getMappedPort(PORT);
    }
}
