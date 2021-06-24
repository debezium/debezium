/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.pulsar;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class PulsarTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final String PULSAR_VERSION = "2.5.2";
    public static final int PULSAR_PORT = 6650;
    public static final int PULSAR_HTTP_PORT = 8080;
    public static final String PULSAR_IMAGE = "apachepulsar/pulsar:" + PULSAR_VERSION;

    private static final GenericContainer<?> container = new GenericContainer<>(PULSAR_IMAGE)
            .withStartupTimeout(Duration.ofSeconds(90))
            .waitingFor(Wait.forLogMessage(".*messaging service is ready.*", 1))
            .withCommand("bin/pulsar", "standalone")
            .withClasspathResourceMapping("/docker/conf/", "/pulsar/conf", BindMode.READ_ONLY)
            .withExposedPorts(PULSAR_PORT, PULSAR_HTTP_PORT);

    @Override
    public Map<String, String> start() {
        container.start();

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.pulsar.client.serviceUrl", getPulsarServiceUrl());

        return params;
    }

    @Override
    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    public static String getPulsarServiceUrl() {
        return "pulsar://localhost:" + container.getMappedPort(PULSAR_PORT);
    }
}
