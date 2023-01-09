/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.infinispan;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class InfinispanTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinispanTestResourceLifecycleManager.class);
    public static final String INFINISPAN_IMAGE = "quay.io/infinispan/server:14.0.4.Final";
    public static final int PORT = ConfigurationProperties.DEFAULT_HOTROD_PORT;
    public static final String CONFIG_PATH = "/etc/infinispan-local.xml";
    private static final GenericContainer<?> container = new GenericContainer<>(INFINISPAN_IMAGE)
            .withExposedPorts(PORT)
            .withClasspathResourceMapping(InfinispanTestConfigSource.CONFIG_FILE, CONFIG_PATH, BindMode.READ_ONLY)
            .withCommand("-c", CONFIG_PATH)
            .withEnv("USER", InfinispanTestConfigSource.USER_NAME)
            .withEnv("PASS", InfinispanTestConfigSource.PASSWORD);

    private static final AtomicBoolean running = new AtomicBoolean(false);

    private static synchronized void init() {
        if (!running.get()) {
            container.start();
            running.set(true);
        }
    }

    public static String getHost() {
        return container.getHost();
    }

    public static int getPort() {
        return container.getMappedPort(PORT);
    }

    @Override
    public Map<String, String> start() {
        init();

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.infinispan.server.host", getHost());
        params.put("debezium.sink.infinispan.server.port", String.valueOf(getPort()));
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
}
