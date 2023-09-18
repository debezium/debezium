/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import java.util.Properties;

/**
 * Control over Kafka cluster
 *
 * @author Jakub Cechacek
 */
public interface KafkaController {

    /**
     * The Bootstrap address returned by this method must be reachable form arbitrary network.
     * @return Publicly reachable Kafka Bootstrap Server address
     */
    String getPublicBootstrapAddress();

    /**
     * The Bootstrap address returned by this method may not be reachable form arbitrary network.
     * @return Kafka Bootstrap Server address
     */
    String getBootstrapAddress();

    String getTlsBootstrapAddress();

    /**
     * Undeploy this Kafka cluster
     *
     * @return true on operation success
     */
    boolean undeploy();

    /**
     * Waits for cluster to be available
     */
    void waitForCluster() throws InterruptedException;

    /**
     * @return default kafka consumer configuration
     */
    default Properties getDefaultConsumerProperties() {
        Properties consumerProps = new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, getPublicBootstrapAddress());
        consumerProps.put(GROUP_ID_CONFIG, "DEBEZIUM_IT_01");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ENABLE_AUTO_COMMIT_CONFIG, false);

        return consumerProps;
    }
}
