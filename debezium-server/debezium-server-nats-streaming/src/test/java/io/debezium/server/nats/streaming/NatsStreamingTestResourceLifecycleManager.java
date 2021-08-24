/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.streaming;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Manages the lifecycle of a NATS Streaming test resource.
 *
 * @author Thiago Avancini
 */
public class NatsStreamingTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static final int NATS_STREAMING_PORT = 4222;
    public static final String NATS_STREAMING_IMAGE = "nats-streaming:latest";

    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static final GenericContainer<?> container = new GenericContainer<>(NATS_STREAMING_IMAGE)
            .withExposedPorts(NATS_STREAMING_PORT)
            .withCommand("-SD", "-cid", "debezium")
            .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Server is ready.*"));

    private static synchronized void start(boolean ignored) {
        if (!running.get()) {
            container.start();
            running.set(true);
        }
    }

    @Override
    public Map<String, String> start() {
        start(true);
        Map<String, String> params = new ConcurrentHashMap<>();
        return params;
    }

    @Override
    public void stop() {
        try {
            container.stop();
        }
        catch (Exception e) {
            // ignored
        }
        running.set(false);
    }

    public static String getNatsStreamingContainerUrl() {
        start(true);
        return String.format("nats://%s:%d", container.getContainerIpAddress(), container.getFirstMappedPort());
    }
}
