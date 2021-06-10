/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.kafka;

/**
 * Control over Kafka cluster
 *
 * @author Jakub Cechacek
 */
public interface KafkaController {

    /**
     * @return host and port for public bootstrap service
     */
    String getKafkaBootstrapAddress();

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
}
