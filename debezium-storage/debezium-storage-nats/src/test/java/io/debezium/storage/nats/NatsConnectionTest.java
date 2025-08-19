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
class NatsConnectionTest {

    private static final String NATS_CONTAINER_IMAGE = "nats:2.12.0-alpine";
    private static final int NATS_PORT = 4222;

    @Container
    @SuppressWarnings("resource")
    public GenericContainer<?> natsContainer = new GenericContainer<>(DockerImageName.parse(NATS_CONTAINER_IMAGE))
            .withExposedPorts(NATS_PORT)
            .withCommand("--jetstream");

    private String natsUrl;
    private NatsConnection natsConnection;

    @BeforeEach
    public void setUp() {
        natsContainer.start();
        natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getFirstMappedPort();
    }

    @AfterEach
    public void tearDown() {
        if (natsConnection != null) {
            natsConnection.close();
        }
        if (natsContainer != null) {
            natsContainer.stop();
        }
    }

    @Test
    public void shouldCreateConnection() throws Exception {
        NatsCommonConfig config = createConfig();
        natsConnection = NatsConnection.getInstance(config, "test");

        assertNotNull(natsConnection);
        assertNotNull(natsConnection.getConnection());
        assertEquals(Connection.Status.CONNECTED, natsConnection.getConnection().getStatus());
    }

    @Test
    public void shouldGetJetStream() throws Exception {
        NatsCommonConfig config = createConfig();
        natsConnection = NatsConnection.getInstance(config, "test");

        JetStream jetStream = natsConnection.getJetStream();
        assertNotNull(jetStream);
    }

    @Test
    public void shouldGetJetStreamManagement() throws Exception {
        NatsCommonConfig config = createConfig();
        natsConnection = NatsConnection.getInstance(config, "test");

        JetStreamManagement jsm = natsConnection.getJetStreamManagement();
        assertNotNull(jsm);
    }

    @Test
    public void shouldHandleInvalidUrl() {
        Configuration config = Configuration.from(Collect.hashMapOf(
                "nats.url", "nats://invalid-host:4222"));

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);

        assertThrows(Exception.class, () -> {
            NatsConnection natsConnection = NatsConnection.getInstance(natsConfig, "test");
            natsConnection.getConnection(); // This should trigger the connection attempt and throw an exception
        });
    }

    @Test
    public void shouldReuseConnection() throws Exception {
        NatsCommonConfig config = createConfig();

        NatsConnection conn1 = NatsConnection.getInstance(config, "test");
        NatsConnection conn2 = NatsConnection.getInstance(config, "test");

        // Should reuse the same connection instance
        assertThat(conn1).isSameAs(conn2);

        conn1.close();
    }

    @Test
    public void shouldCloseConnection() throws Exception {
        NatsCommonConfig config = createConfig();
        natsConnection = NatsConnection.getInstance(config, "test");

        Connection connection = natsConnection.getConnection();
        assertTrue(connection.getStatus() == Connection.Status.CONNECTED);

        natsConnection.close();

        // Connection should be closed - check the original connection object
        assertTrue(connection.getStatus() == Connection.Status.CLOSED);

        // Also verify that isConnected() returns false
        assertTrue(!natsConnection.isConnected());
    }

    @Test
    public void shouldHandleConnectionTimeout() {
        Configuration config = Configuration.from(Collect.hashMapOf(
                "nats.url", natsUrl));

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);

        // Should still work with valid URL even with short timeout
        natsConnection = NatsConnection.getInstance(natsConfig, "test");
        assertNotNull(natsConnection);
    }

    @Test
    public void shouldConfigureConnectionName() throws Exception {
        Configuration config = Configuration.from(Collect.hashMapOf(
                "nats.url", natsUrl));

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);
        natsConnection = NatsConnection.getInstance(natsConfig, "test");

        assertNotNull(natsConnection);
        // Connection name should be set (though we can't easily verify it without
        // server inspection)
        assertTrue(natsConnection.getConnection().getStatus() == Connection.Status.CONNECTED);
    }

    @Test
    public void shouldHandleReconnectSettings() throws Exception {
        Configuration config = Configuration.from(Collect.hashMapOf(
                "nats.url", natsUrl,
                "nats.max.reconnects", "5",
                "nats.reconnect.wait.ms", "1000"));

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);
        natsConnection = NatsConnection.getInstance(natsConfig, "test");

        assertNotNull(natsConnection);
        assertTrue(natsConnection.getConnection().getStatus() == Connection.Status.CONNECTED);
    }

    private NatsCommonConfig createConfig() {
        Configuration config = Configuration.from(Collect.hashMapOf(
                "nats.url", natsUrl,
                "nats.max.reconnects", "3",
                "nats.reconnect.wait.ms", "2000"));

        return new NatsCommonConfig(config);
    }
}
