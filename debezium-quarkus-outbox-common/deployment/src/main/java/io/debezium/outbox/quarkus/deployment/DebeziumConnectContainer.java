/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.deployment;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.lang3.Validate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.devservices.common.ConfigureUtil;

final class DebeziumConnectContainer extends GenericContainer<DebeziumConnectContainer> {

    private static final Integer DEBEZIUM_REST_API_PORT = 8083;

    public static final String HOST_DOCKER_INTERNAL = "host.docker.internal";

    DebeziumConnectContainer(final DockerImageName dockerImageName,
                             final DebeziumConnectorDevService.HostInternalKafkaBootstrapServer hostInternalKafkaBootstrapServer) {
        super(dockerImageName);
        Validate.validState(dockerImageName.isCompatibleWith(DockerImageName.parse("quay.io/debezium/connect")));
        Objects.requireNonNull(hostInternalKafkaBootstrapServer);
        this.withExposedPorts(DEBEZIUM_REST_API_PORT);
        this.withExtraHost(HOST_DOCKER_INTERNAL, "host-gateway");
        this.withEnv(
                Map.of(
                        "BOOTSTRAP_SERVERS", hostInternalKafkaBootstrapServer.toHostPort(),
                        "KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter",
                        "VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter",
                        "GROUP_ID", "1",
                        "CONFIG_STORAGE_TOPIC", "my_connect_configs",
                        "OFFSET_STORAGE_TOPIC", "my_connect_offsets",
                        "STATUS_STORAGE_TOPIC", "my_connect_statuses"));
        this.withLogConsumer(frameConsumer());
        this.waitingFor(Wait.forLogMessage(".*Finished starting connectors and tasks.*", 1));
    }

    @Override
    protected void configure() {
        super.configure();
        ConfigureUtil.configureSharedNetwork(this, "debezium");
    }

    private Consumer<OutputFrame> frameConsumer() {
        return frame -> logger().info(frame.getUtf8String().stripTrailing());
    }

    public Integer getDebeziumRestApiHostPort() {
        return Objects.requireNonNull(this.getMappedPort(DEBEZIUM_REST_API_PORT));
    }
}
