package io.debezium.testing.openshift.tools.kafka;

import java.io.IOException;

import io.strimzi.api.kafka.model.KafkaConnect;

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
     * Deploys connector
     *
     * @param name connector's name
     * @param config connector's configuration
     */
    void deployConnector(String name, ConnectorConfigBuilder config) throws IOException, InterruptedException;

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
     * @return url of metrics endpoint
     */
    HttpUrl getMetricsURL();

    /**
     * Waits for cluster to be available
     */
    KafkaConnect waitForCluster() throws InterruptedException;

    /**
     * Undeploy this Kafka connect cluster
     *
     * @return true on operation success
     */
    boolean undeploy();

    void waitForMySqlSnapshot(String connectorName) throws IOException;

    void waitForPostgreSqlSnapshot(String connectorName) throws IOException;

    void waitForSqlServerSnapshot(String connectorName) throws IOException;

    void waitForMongoSnapshot(String connectorName) throws IOException;

    void waitForDB2Snapshot(String connectorName) throws IOException;
}
