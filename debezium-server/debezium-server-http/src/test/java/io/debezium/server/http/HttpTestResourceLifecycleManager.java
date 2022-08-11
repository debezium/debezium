/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.http;

import static java.net.HttpURLConnection.HTTP_OK;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class HttpTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTestResourceLifecycleManager.class);
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

    public static void reset() {
        try {
            HttpClient client = HttpClient.newHttpClient();
            String resetURL = "http://" + container.getHost() + ":" + container.getMappedPort(PORT) + "/__admin/reset";
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(new URI(resetURL)).timeout(Duration.ofMillis(60_000));
            HttpRequest request = requestBuilder.POST(HttpRequest.BodyPublishers.ofString("")).build();
            HttpResponse r = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() != HTTP_OK) {
                throw new IllegalStateException("Get wrong response while resetting WireMock: " + r.statusCode());
            }
            LOGGER.info("WireMock reset");
        }
        catch (Exception e) {
            LOGGER.warn("Failed to reset WireMock", e);
        }
    }
}
