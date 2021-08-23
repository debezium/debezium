/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.http;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testcontainers.containers.GenericContainer;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class HttpTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
    public static final String WIREMOCK_IMAGE = "docker.io/wiremock/wiremock:latest";
    public static final int PORT = 8080; // Primary port used by wiremock
    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static final GenericContainer<?> container = new GenericContainer<>(WIREMOCK_IMAGE)
            .withExposedPorts(PORT);

    private static synchronized void init() {
        if (!running.get()) {
            container.start();
            running.set(true);
        }
    }

    @Override
    public Map<String, String> start() {
        init();

        return Collections.singletonMap("debezium.sink.http.url", getURL());
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

    private String getURL() {
        return "http://" + container.getHost() + ":" + container.getMappedPort(PORT);
    }

    public static String getHost() {
        return container.getHost();
    }

    public static int getPort() {
        return container.getMappedPort(PORT);
    }
}
