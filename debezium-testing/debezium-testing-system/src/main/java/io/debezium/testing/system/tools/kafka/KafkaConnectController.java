/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import java.io.IOException;

import io.debezium.testing.system.tools.kafka.connectors.ConnectorMetricsReader;

import okhttp3.HttpUrl;

/**
 * Control over Kafka Connect cluster
 *
 * @author Jakub Cechacek
 */
public interface KafkaConnectController {

    /**
     * Disables Kafka Connect
     */
    void disable();

    /**
     * Crashes Kafka Connect
     */
    void destroy();

    /**
     * Restores Kafka Connect cluster after a call to {@link #disable()} or {@link #destroy()}
     */
    void restore() throws InterruptedException;

    /**
     * Deploys connector
     *
     * @param config connector's configuration
     */
    void deployConnector(ConnectorConfigBuilder config) throws IOException, InterruptedException;

    /**
     * Undeploys connector
     *
     * @param name name of the connector
     */
    void undeployConnector(String name) throws IOException;

    /**
     * @return url of KC http API
     */
    HttpUrl getApiURL();

    /**
     * Waits for cluster to be available
     */
    void waitForCluster() throws InterruptedException;

    /**
     * Undeploy this Kafka connect cluster
     *
     * @return true on operation success
     */
    boolean undeploy();

    /**
     * @return metrics reader for this kafka connect
     */
    ConnectorMetricsReader getMetricsReader();
}
