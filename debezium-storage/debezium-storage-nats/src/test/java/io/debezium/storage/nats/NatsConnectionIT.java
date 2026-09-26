/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private static final String NATS_CONTAINER_IMAGE = "nats:2.12.0-alpine";
    private static final int NATS_PORT = 4222;

    @Container
    @SuppressWarnings("resource")
    public GenericContainer<?> natsContainer = new GenericContainer<>(DockerImageName.parse(NATS_CONTAINER_IMAGE))
            .withExposedPorts(NATS_PORT)
            .withCommand("-js")
            .withLogConsumer(frame -> {
                if (frame != null && frame.getUtf8String() != null) {
                    System.out.print(frame.getUtf8String());
                }
            });

    private String natsUrl;
    private NatsConnection natsConnection;

    @BeforeEach
    public void setUp() {
        natsUrl = "nats://%s:%d".formatted(natsContainer.getHost(), natsContainer.getMappedPort(NATS_PORT));
    }

    @AfterEach
    public void tearDown() {
        if (natsConnection != null) {
            natsConnection.close();
        }
    }

    @Test
    public void shouldCreateConnection() throws Exception {
        NatsCommonConfig config = createConfig();
        natsConnection = new NatsConnection(config);

        assertNotNull(natsConnection);
        assertNotNull(natsConnection.getConnection());
        assertEquals(Connection.Status.CONNECTED, natsConnection.getConnection().getStatus());
    }

    @Test
    public void shouldGetJetStream() throws Exception {
        NatsCommonConfig config = createConfig();
        natsConnection = new NatsConnection(config);

        JetStream jetStream = natsConnection.getJetStream();
        assertNotNull(jetStream);
    }

    @Test
    public void shouldGetJetStreamManagement() throws Exception {
        NatsCommonConfig config = createConfig();
        natsConnection = new NatsConnection(config);

        JetStreamManagement jsm = natsConnection.getJetStreamManagement();
        assertNotNull(jsm);
    }

    @Test
    public void shouldHandleInvalidUrl() {
        Configuration config = Configuration.from(Collect.hashMapOf(
                "nats.url", "nats://invalid-host:4222"));

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);

        assertThrows(Exception.class, () -> {
            NatsConnection natsConnection = new NatsConnection(natsConfig);
            natsConnection.getConnection(); // This should trigger the connection attempt and throw an exception
        });
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
        assertTrue(connection.getStatus() == Connection.Status.CONNECTED);

        natsConnection.close();

        // Connection should be closed - check the original connection object
        assertTrue(connection.getStatus() == Connection.Status.CLOSED);

        // Also verify that isConnected() returns false
        assertTrue(!natsConnection.isConnected());
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

        assertThrows(Exception.class, connection::getConnection);
    }

    @Test
    public void shouldHandleReconnectSettings() throws Exception {
        Configuration config = Configuration.from(Collect.hashMapOf(
                "nats.url", natsUrl,
                "nats.max.reconnects", "5",
                "nats.reconnect.wait.ms", "1000"));

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);
        natsConnection = new NatsConnection(natsConfig);

        assertNotNull(natsConnection);
        assertTrue(natsConnection.getConnection().getStatus() == Connection.Status.CONNECTED);
    }

    @Test
    public void shouldConnectWithUserPassword() throws Exception {
        try (GenericContainer<?> authNats = new GenericContainer<>(DockerImageName.parse(NATS_CONTAINER_IMAGE))
                .withExposedPorts(NATS_PORT)
                .withCommand("-js", "--user", "debezium", "--pass", "secret")) {
            authNats.start();
            String url = "nats://%s:%d".formatted(authNats.getHost(), authNats.getMappedPort(NATS_PORT));

            NatsCommonConfig natsConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                    "nats.url", url,
                    "nats.user", "debezium",
                    "nats.password", "secret")), "");
            NatsConnection conn = new NatsConnection(natsConfig);
            try {
                assertEquals(Connection.Status.CONNECTED, conn.getConnection().getStatus());
            }
            finally {
                conn.close();
            }
        }
    }

    @Test
    public void shouldConnectWithToken() throws Exception {
        try (GenericContainer<?> authNats = new GenericContainer<>(DockerImageName.parse(NATS_CONTAINER_IMAGE))
                .withExposedPorts(NATS_PORT)
                .withCommand("-js", "--auth", "tokensecret")) {
            authNats.start();
            String url = "nats://%s:%d".formatted(authNats.getHost(), authNats.getMappedPort(NATS_PORT));

            NatsCommonConfig natsConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                    "nats.url", url,
                    "nats.token", "tokensecret")), "");
            NatsConnection conn = new NatsConnection(natsConfig);
            try {
                assertEquals(Connection.Status.CONNECTED, conn.getConnection().getStatus());
            }
            finally {
                conn.close();
            }
        }
    }

    @Test
    public void shouldFailToConnectWithoutCredentials() {
        try (GenericContainer<?> authNats = new GenericContainer<>(DockerImageName.parse(NATS_CONTAINER_IMAGE))
                .withExposedPorts(NATS_PORT)
                .withCommand("-js", "--user", "debezium", "--pass", "secret")) {
            authNats.start();
            String url = "nats://%s:%d".formatted(authNats.getHost(), authNats.getMappedPort(NATS_PORT));

            NatsCommonConfig natsConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                    "nats.url", url)), "");
            NatsConnection conn = new NatsConnection(natsConfig);
            try {
                assertThrows(Exception.class, conn::getConnection);
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
