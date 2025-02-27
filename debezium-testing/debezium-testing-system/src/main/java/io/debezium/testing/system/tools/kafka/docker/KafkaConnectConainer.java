/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.docker;

import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.WaitConditions;

public class KafkaConnectConainer extends GenericContainer<KafkaConnectConainer> {

    public static final String KAFKA_CONNECT_COMMAND = "kafka-connect";
    public static final String KAFKA_JMX_HOST = "0.0.0.0";
    public static final int KAFKA_CONNECT_API_PORT = 8083;
    public static final int PROMETHEUS_METRICS_PORT = 9404;
    public static final int KAFKA_JMX_PORT = 9999;

    public KafkaConnectConainer(String containerImageName) {
        super(containerImageName);
        defaultConfig();
    }

    public KafkaConnectConainer() {
        this(ConfigProperties.DOCKER_IMAGE_KAFKA_RHEL);
    }

    private void defaultConfig() {
        withReuse(false);
        withExposedPorts(KAFKA_CONNECT_API_PORT, KAFKA_JMX_PORT);
        addEnv("CONFIG_STORAGE_TOPIC", "connect_config");
        addEnv("OFFSET_STORAGE_TOPIC", "connect_offsets");
        addEnv("STATUS_STORAGE_TOPIC", "connect_statuses");
        addEnv("JMXHOST", KAFKA_JMX_HOST);
        addEnv("JMXPORT", String.valueOf(KAFKA_JMX_PORT));
        withHttpMetrics();
        withStartupTimeout(Duration.ofMinutes(WaitConditions.scaled(1)));
        withCommand(KAFKA_CONNECT_COMMAND);
    }

    public KafkaConnectConainer withKafka(KafkaContainer kafka) {
        return this
                .dependsOn(kafka)
                .withKafka(kafka.getNetwork(), kafka.getBootstrapAddress());
    }

    public KafkaConnectConainer withKafka(Network network, String bootstrapServers) {
        return this
                .withNetwork(network)
                .withEnv("BOOTSTRAP_SERVERS", bootstrapServers);
    }

    public KafkaConnectConainer withHttpMetrics() {
        addEnv("ENABLE_JMX_EXPORTER", "true");
        addExposedPort(PROMETHEUS_METRICS_PORT);
        return this;
    }

}
