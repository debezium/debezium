/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import io.strimzi.test.container.StrimziKafkaContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    private static final Integer SCHEMA_REGISTRY_PORT = 8080;

    private static DockerImageName schemaRegistryImage() {
        final String baseImageName = "quay.io/apicurio/apicurio-registry";
        final String apicurioVersion = System.getProperty("version.apicurio", "2.6.13.Final");
        String imageName = apicurioVersion.startsWith("2.")
                ? baseImageName + "-mem"
                : baseImageName;

        return DockerImageName
                .parse(imageName)
                .withTag(apicurioVersion);
    }

    SchemaRegistryContainer() {
        super(schemaRegistryImage());
        addExposedPorts(SCHEMA_REGISTRY_PORT);
    }

    public SchemaRegistryContainer withKafka(StrimziKafkaContainer kafkaContainer) {
        return withKafka(kafkaContainer.getNetwork(), kafkaContainer.getNetworkAliases().getFirst() + ":" + StrimziKafkaContainer.KAFKA_PORT);
    }

    public SchemaRegistryContainer withKafka(Network network, String bootstrapServers) {
        withNetwork(network);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT);
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServers);
        return self();
    }
}
