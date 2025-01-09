/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    private static final String SCHEMA_REGISTRY_DOCKER_IMAGE_NAME = "quay.io/debezium/confluentinc-cp-schema-registry:6.0.2";
    private static final DockerImageName SCHEMA_REGISTRY_DOCKER_IMAGE = DockerImageName.parse(SCHEMA_REGISTRY_DOCKER_IMAGE_NAME)
            .asCompatibleSubstituteFor("confluentinc/cp-schema-registry");
    private static final Integer SCHEMA_REGISTRY_EXPOSED_PORT = 8081;

    SchemaRegistryContainer() {
        super(SCHEMA_REGISTRY_DOCKER_IMAGE);
        addExposedPorts(SCHEMA_REGISTRY_EXPOSED_PORT);
    }

    public SchemaRegistryContainer withKafka(KafkaContainer kafkaContainer) {
        return withKafka(kafkaContainer.getNetwork(), kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    public SchemaRegistryContainer withKafka(Network network, String bootstrapServers) {
        withNetwork(network);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081");
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServers);
        return self();
    }
}
