/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import java.util.function.Consumer;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

/**
 * A NATS server with JetStream enabled, configured the way every NATS storage
 * test needs it. Tests that need a differently configured server (authentication,
 * TLS, or no JetStream at all) build their own container, but reuse the constants
 * and helpers here so all of them stay consistent.
 *
 * @author Nick Chomey
 */
public class NatsContainer extends GenericContainer<NatsContainer> {

    /**
     * The version of the NATS server the tests run against. Defaults to the newest 2.x
     * release; override with {@code -Dversion.nats.server=2.15.0-alpine} to pin one. The
     * {@code assembly} profile forwards the Maven property of the same name to the forked
     * test JVM.
     */
    public static final String IMAGE_VERSION = System.getProperty("version.nats.server", "2-alpine");

    public static final String IMAGE = "nats:" + IMAGE_VERSION;
    public static final int NATS_PORT = 4222;
    public static final int NATS_MONITOR_PORT = 8222;

    public NatsContainer() {
        super(DockerImageName.parse(IMAGE));
        withExposedPorts(NATS_PORT);
        withCommand("-js");
        withLogConsumer(logToStdout());
    }

    /**
     * Forwards a container's output to stdout, so that a failing test shows the
     * server log.
     */
    public static Consumer<OutputFrame> logToStdout() {
        return frame -> {
            if (frame != null && frame.getUtf8String() != null) {
                System.out.print(frame.getUtf8String());
            }
        };
    }

    /**
     * The URL the server can be reached on from the host.
     */
    public static String serverUrl(GenericContainer<?> container) {
        return "nats://%s:%d".formatted(container.getHost(), container.getMappedPort(NATS_PORT));
    }

    public String getServerUrl() {
        return serverUrl(this);
    }
}
