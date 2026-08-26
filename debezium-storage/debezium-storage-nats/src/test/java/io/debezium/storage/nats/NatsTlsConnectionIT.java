/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.debezium.config.Configuration;
import io.debezium.util.Collect;
import io.nats.client.Connection;

/**
 * Verifies a real TLS handshake against a NATS server configured with a
 * self-signed certificate. Requires {@code openssl} and {@code keytool} on
 * the host; the test is skipped when they are unavailable.
 *
 * @author Nick Chomey
 */
class NatsTlsConnectionIT {

    private static final String NATS_CONTAINER_IMAGE = "nats:2.12.0-alpine";
    private static final int NATS_PORT = 4222;

    @TempDir
    Path tempDir;

    @Test
    @Timeout(60)
    public void shouldConnectWithTls() throws Exception {
        assumeTrue(isToolAvailable("openssl"), "openssl not available, skipping TLS test");
        assumeTrue(isToolAvailable("keytool"), "keytool not available, skipping TLS test");

        // Generate a self-signed certificate for the server
        File certFile = tempDir.resolve("server.crt").toFile();
        File keyFile = tempDir.resolve("server.key").toFile();
        runProcess("openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", keyFile.getAbsolutePath(),
                "-out", certFile.getAbsolutePath(),
                "-days", "1", "-nodes", "-subj", "/CN=localhost");

        // Build a JKS truststore containing the certificate
        File truststoreFile = tempDir.resolve("truststore.jks").toFile();
        runProcess("keytool", "-importcert", "-noprompt", "-alias", "nats",
                "-file", certFile.getAbsolutePath(),
                "-keystore", truststoreFile.getAbsolutePath(),
                "-storepass", "changeit");

        try (GenericContainer<?> tlsNats = new GenericContainer<>(DockerImageName.parse(NATS_CONTAINER_IMAGE))
                .withExposedPorts(NATS_PORT)
                .withCopyFileToContainer(MountableFile.forHostPath(certFile.getAbsolutePath()), "/certs/server.crt")
                .withCopyFileToContainer(MountableFile.forHostPath(keyFile.getAbsolutePath()), "/certs/server.key")
                .withCommand("--jetstream", "--tls", "--tlscert", "/certs/server.crt", "--tlskey", "/certs/server.key")) {
            tlsNats.start();
            String url = "nats://" + tlsNats.getHost() + ":" + tlsNats.getFirstMappedPort();

            NatsCommonConfig natsConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                    "nats.url", url,
                    "nats.tls.enabled", "true",
                    "nats.tls.truststore.path", truststoreFile.getAbsolutePath(),
                    "nats.tls.truststore.password", "changeit")), "");
            NatsConnection conn = NatsConnection.getInstance(natsConfig, "tls-handshake-test");
            try {
                assertEquals(Connection.Status.CONNECTED, conn.getConnection().getStatus());
            }
            finally {
                conn.close();
            }
        }
    }

    private static boolean isToolAvailable(String tool) {
        try {
            Process process = new ProcessBuilder(tool, "-help").redirectErrorStream(true).start();
            process.getInputStream().readAllBytes();
            process.waitFor();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private static void runProcess(String... command) throws Exception {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IllegalStateException("Command failed (" + exit + "): " + String.join(" ", command) + "\n" + output);
        }
    }
}