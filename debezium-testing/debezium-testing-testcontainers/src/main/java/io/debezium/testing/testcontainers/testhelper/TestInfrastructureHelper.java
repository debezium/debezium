/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.testhelper;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.PullPolicy;

import io.debezium.testing.testcontainers.OracleContainer;
import io.strimzi.test.container.StrimziKafkaCluster;

public class TestInfrastructureHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestInfrastructureHelper.class);

    public static final int CI_CONTAINER_STARTUP_TIME = 90;
    private static final Network NETWORK = Network.newNetwork();

    public static StrimziKafkaCluster createKafkaCluster() {
        StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withContainerCustomizer(container -> {
                    container.withNetwork(NETWORK)
                            .withImagePullPolicy(PullPolicy.ageBased(Duration.ofHours(8)));
                })
                .build();

        return kafkaCluster;
    }

    private static final StrimziKafkaCluster KAFKA_CONTAINER = createKafkaCluster();

    private static final OracleContainer ORACLE_CONTAINER = (OracleContainer) new OracleContainer()
            .withImagePullPolicy(PullPolicy.ageBased(Duration.ofHours(8)))
            .withNetwork(NETWORK)
            .withNetworkAliases("oracledb")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));

    public static Network getNetwork() {
        return NETWORK;
    }

    public static StrimziKafkaCluster getKafkaCluster() {
        return KAFKA_CONTAINER;
    }

    public static OracleContainer getOracleContainer() {
        return ORACLE_CONTAINER;
    }

}
