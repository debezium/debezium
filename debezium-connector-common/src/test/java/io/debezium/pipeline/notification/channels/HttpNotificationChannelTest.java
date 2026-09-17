/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification.channels;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.notification.Notification;

@ExtendWith(MockitoExtension.class)
public class HttpNotificationChannelTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Mock
    private CommonConnectorConfig connectorConfig;

    private HttpServer server;
    private final AtomicInteger requestCount = new AtomicInteger();
    private final List<String> receivedBodies = new CopyOnWriteArrayList<>();
    private volatile int responseStatus = 200;
    private volatile long responseDelayMillis = 0;

    private final HttpNotificationChannel channel = new HttpNotificationChannel();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/notify", new RecordingHandler());
        server.start();
    }

    @AfterEach
    void stopServer() {
        channel.close();
        server.stop(0);
    }

    private String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/notify";
    }

    private void initChannel(String url, int retries) {
        // The test server binds to loopback, so private-network targets must be allowed for delivery to happen.
        initChannel(url, retries, true);
    }

    private void initChannel(String url, int retries, boolean allowPrivateNetworks) {
        lenient().when(connectorConfig.getNotificationHttpUrl()).thenReturn(url);
        lenient().when(connectorConfig.getNotificationHttpRetries()).thenReturn(retries);
        lenient().when(connectorConfig.getNotificationHttpTimeoutMs()).thenReturn(5000);
        lenient().when(connectorConfig.isNotificationHttpAllowPrivateNetworks()).thenReturn(allowPrivateNetworks);
        channel.init(connectorConfig);
    }

    private Notification notification(String type) {
        return new Notification("id-1", "Initial Snapshot", type,
                Map.of("connector_name", "inventory-connector"), 1695817046353L);
    }

    @Test
    void nameIsHttp() {
        assertThat(channel.name()).isEqualTo("http");
    }

    @Test
    void shouldSerializeAndPostNotification() throws Exception {
        initChannel(url(), 2);
        channel.send(notification("STARTED"));
        flush();

        assertThat(requestCount.get()).isEqualTo(1);
        JsonNode json = MAPPER.readTree(receivedBodies.get(0));
        assertThat(json.get("id").asText()).isEqualTo("id-1");
        assertThat(json.get("aggregateType").asText()).isEqualTo("Initial Snapshot");
        assertThat(json.get("type").asText()).isEqualTo("STARTED");
        assertThat(json.get("timestamp").asLong()).isEqualTo(1695817046353L);
        assertThat(json.get("additionalData").get("connector_name").asText()).isEqualTo("inventory-connector");
    }

    @Test
    void shouldNotThrowWhenDeliveryFails() {
        responseStatus = 500;
        initChannel(url(), 0); // retries=0 -> single attempt, no backoff wait
        assertThatCode(() -> channel.send(notification("IN_PROGRESS"))).doesNotThrowAnyException();
        flush();
        assertThat(requestCount.get()).isEqualTo(1);
    }

    @Test
    void shouldNotBlockCallerWhenEndpointIsSlow() {
        responseDelayMillis = 2000;
        initChannel(url(), 0);
        long startNanos = System.nanoTime();
        channel.send(notification("IN_PROGRESS"));
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
        // send() must return promptly by dispatching to the executor rather than waiting for the slow endpoint.
        assertThat(elapsedMillis).isLessThan(1000);
    }

    @Test
    void shouldNotContactPrivateTargetWhenPrivateNetworksDisallowed() {
        // Default allowPrivateNetworks=false: the SSRF guard rejects the loopback target before any request is sent.
        initChannel(url(), 0, false);
        assertThatCode(() -> channel.send(notification("STARTED"))).doesNotThrowAnyException();
        flush();
        assertThat(requestCount.get()).isZero();
    }

    @Test
    void shouldBeNoOpWhenUrlNotConfigured() {
        initChannel(null, 2);
        assertThatCode(() -> channel.send(notification("STARTED"))).doesNotThrowAnyException();
        flush();
        assertThat(requestCount.get()).isZero();
    }

    @Test
    void nonTerminalMaxAttemptsIsRetriesPlusOne() {
        lenient().when(connectorConfig.getNotificationHttpRetries()).thenReturn(2);
        lenient().when(connectorConfig.getNotificationHttpTimeoutMs()).thenReturn(5000);
        channel.init(connectorConfig);
        assertThat(channel.maxAttemptsFor(notification("IN_PROGRESS"))).isEqualTo(3);
    }

    @Test
    void terminalMaxAttemptsEscalates() {
        lenient().when(connectorConfig.getNotificationHttpRetries()).thenReturn(1);
        lenient().when(connectorConfig.getNotificationHttpTimeoutMs()).thenReturn(5000);
        channel.init(connectorConfig);
        // terminal escalates to max(retries, 3) + 1 = 4, even though retries=1 would give only 2
        assertThat(channel.maxAttemptsFor(notification("COMPLETED"))).isEqualTo(4);
        assertThat(channel.maxAttemptsFor(notification("ABORTED"))).isEqualTo(4);
        assertThat(channel.maxAttemptsFor(notification("SKIPPED"))).isEqualTo(4);
    }

    @Test
    void urlValidationPassesWhenHttpChannelDisabled() {
        assertThat(urlValid(Configuration.create().build())).isTrue();
    }

    @Test
    void urlValidationFailsWhenHttpEnabledWithoutUrl() {
        assertThat(urlValid(Configuration.create().with("notification.enabled.channels", "http").build())).isFalse();
    }

    @Test
    void urlValidationFailsForMalformedUrl() {
        assertThat(urlValid(Configuration.create()
                .with("notification.enabled.channels", "http")
                .with("notification.http.url", "not a url").build())).isFalse();
    }

    @Test
    void urlValidationFailsForNonHttpScheme() {
        assertThat(urlValid(Configuration.create()
                .with("notification.enabled.channels", "http")
                .with("notification.http.url", "ftp://example.com/notify").build())).isFalse();
    }

    @Test
    void urlValidationPassesForValidHttpUrl() {
        assertThat(urlValid(Configuration.create()
                .with("notification.enabled.channels", "http")
                .with("notification.http.url", "https://example.com/notify").build())).isTrue();
    }

    private final class RecordingHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            requestCount.incrementAndGet();
            try (InputStream in = exchange.getRequestBody()) {
                receivedBodies.add(new String(in.readAllBytes(), StandardCharsets.UTF_8));
            }
            if (responseDelayMillis > 0) {
                try {
                    Thread.sleep(responseDelayMillis);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            exchange.sendResponseHeaders(responseStatus, -1);
            exchange.close();
        }
    }

    // Delivery is asynchronous; close() shuts the executor down and awaits termination, flushing pending deliveries.
    private void flush() {
        channel.close();
    }

    private static boolean urlValid(Configuration config) {
        return HttpNotificationChannel.NOTIFICATION_URL.validate(config, (f, v, msg) -> {
        });
    }
}
