/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.util.Collect;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;

/**
 * Tests for NATS connection management.
 *
 * @author Nick Chomey
 */
@Testcontainers
class NatsConnectionIT {

    @Container
    @SuppressWarnings("resource")
    public NatsContainer natsContainer = new NatsContainer();

    private String natsUrl;
    private NatsConnection natsConnection;

    @BeforeEach
    public void setUp() {
        natsUrl = natsContainer.getServerUrl();
    }

    @AfterEach
    public void tearDown() {
        if (natsConnection != null) {
            natsConnection.close();
        }
    }

    @Test
    public void shouldCreateConnection() throws Exception {
        // createConfig() also sets the reconnect settings, so this covers the reconnect
        // configuration path as well.
        NatsCommonConfig config = createConfig();
        natsConnection = new NatsConnection(config);

        assertThat(natsConnection).isNotNull();
        assertThat(natsConnection.getConnection()).isNotNull();
        assertThat(natsConnection.getConnection().getStatus()).isEqualTo(Connection.Status.CONNECTED);
    }

    @Test
    public void shouldGetJetStream() throws Exception {
        NatsCommonConfig config = createConfig();
        natsConnection = new NatsConnection(config);

        JetStream jetStream = natsConnection.getJetStream();
        assertThat(jetStream).isNotNull();
    }

    @Test
    public void shouldGetJetStreamManagement() throws Exception {
        NatsCommonConfig config = createConfig();
        natsConnection = new NatsConnection(config);

        JetStreamManagement jsm = natsConnection.getJetStreamManagement();
        assertThat(jsm).isNotNull();
    }

    @Test
    public void shouldHandleInvalidUrl() {
        Configuration config = Configuration.from(Collect.hashMapOf(
                "nats.url", "nats://invalid-host:4222"));

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);

        assertThatThrownBy(() -> {
            NatsConnection natsConnection = new NatsConnection(natsConfig);
            natsConnection.getConnection(); // This should trigger the connection attempt and throw an exception
        }).isInstanceOf(Exception.class);
    }

    @Test
    public void shouldNotShareConnectionsBetweenCallers() {
        // Each caller owns its connection and closes it when done. There is no
        // shared instance cache, so two requests always produce distinct
        // connections with independent lifecycles.
        NatsCommonConfig config = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                "nats.url", "nats://localhost:4222")), "");

        NatsConnection first = new NatsConnection(config);
        NatsConnection second = new NatsConnection(config);
        try {
            assertThat(first).isNotSameAs(second);
        }
        finally {
            first.close();
            second.close();
        }
    }

    @Test
    public void shouldCloseConnection() throws Exception {
        NatsCommonConfig config = createConfig();
        natsConnection = new NatsConnection(config);

        Connection connection = natsConnection.getConnection();
        assertThat(connection.getStatus()).isEqualTo(Connection.Status.CONNECTED);

        natsConnection.close();

        // Connection should be closed - check the original connection object
        assertThat(connection.getStatus()).isEqualTo(Connection.Status.CLOSED);

        // Also verify that isConnected() returns false
        assertThat(natsConnection.isConnected()).isFalse();
    }

    @Test
    @Timeout(30)
    public void shouldFailToConnectWhenTheServerDoesNotRespond() {
        // 192.0.2.1 is TEST-NET-1 (RFC 5737), which is not routable, so the connection
        // attempt is dropped rather than refused and can only fail through the configured
        // connection timeout. Disabling reconnects keeps that failure prompt.
        Configuration config = Configuration.from(Collect.hashMapOf(
                "nats.url", "nats://192.0.2.1:4222",
                "nats.connection.timeout.ms", "500",
                "nats.max.reconnects", "0"));

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);
        NatsConnection connection = new NatsConnection(natsConfig);

        assertThatThrownBy(connection::getConnection).isInstanceOf(Exception.class);
    }

    @Test
    public void shouldConnectWithUserPassword() throws Exception {
        try (GenericContainer<?> authNats = new GenericContainer<>(DockerImageName.parse(NatsContainer.IMAGE))
                .withExposedPorts(NatsContainer.NATS_PORT)
                .withCommand("-js", "--user", "debezium", "--pass", "secret")
                .withLogConsumer(NatsContainer.logToStdout())) {
            authNats.start();
            String url = NatsContainer.serverUrl(authNats);

            NatsCommonConfig natsConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                    "nats.url", url,
                    "nats.user", "debezium",
                    "nats.password", "secret")), "");
            NatsConnection conn = new NatsConnection(natsConfig);
            try {
                assertThat(conn.getConnection().getStatus()).isEqualTo(Connection.Status.CONNECTED);
            }
            finally {
                conn.close();
            }
        }
    }

    @Test
    public void shouldConnectWithToken() throws Exception {
        try (GenericContainer<?> authNats = new GenericContainer<>(DockerImageName.parse(NatsContainer.IMAGE))
                .withExposedPorts(NatsContainer.NATS_PORT)
                .withCommand("-js", "--auth", "tokensecret")
                .withLogConsumer(NatsContainer.logToStdout())) {
            authNats.start();
            String url = NatsContainer.serverUrl(authNats);

            NatsCommonConfig natsConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                    "nats.url", url,
                    "nats.token", "tokensecret")), "");
            NatsConnection conn = new NatsConnection(natsConfig);
            try {
                assertThat(conn.getConnection().getStatus()).isEqualTo(Connection.Status.CONNECTED);
            }
            finally {
                conn.close();
            }
        }
    }

    @Test
    public void shouldFailToConnectWithoutCredentials() {
        try (GenericContainer<?> authNats = new GenericContainer<>(DockerImageName.parse(NatsContainer.IMAGE))
                .withExposedPorts(NatsContainer.NATS_PORT)
                .withCommand("-js", "--user", "debezium", "--pass", "secret")
                .withLogConsumer(NatsContainer.logToStdout())) {
            authNats.start();
            String url = NatsContainer.serverUrl(authNats);

            NatsCommonConfig natsConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                    "nats.url", url)), "");
            NatsConnection conn = new NatsConnection(natsConfig);
            try {
                assertThatThrownBy(conn::getConnection).isInstanceOf(Exception.class);
            }
            finally {
                conn.close();
            }
        }
    }

    private NatsCommonConfig createConfig() {
        Configuration config = Configuration.from(Collect.hashMapOf(
                "nats.url", natsUrl,
                "nats.max.reconnects", "3",
                "nats.reconnect.wait.ms", "2000"));

        return new NatsCommonConfig(config);
    }
}
