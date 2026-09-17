/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Tests {@link SsrfSafeHttpClient} delivery and retry behaviour against a real loopback {@link HttpServer} stub.
 * A short retry delay keeps the retry paths fast.
 */
public class SsrfSafeHttpClientDeliveryTest {

    private HttpServer server;
    private final AtomicInteger requestCount = new AtomicInteger();
    private final List<String> receivedBodies = new CopyOnWriteArrayList<>();
    private final List<String> receivedMethods = new CopyOnWriteArrayList<>();
    private final List<String> receivedContentTypes = new CopyOnWriteArrayList<>();
    private volatile int[] statusSequence = { 200 };

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/notify", new RecordingHandler());
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/notify";
    }

    private SsrfSafeHttpClient client(boolean allowPrivateNetworks) {
        return SsrfSafeHttpClient.builder()
                .timeout(Duration.ofSeconds(2))
                .allowPrivateNetworks(allowPrivateNetworks)
                .retryInitialDelay(Duration.ofMillis(1))
                .retryMaxDelay(Duration.ofMillis(2))
                .build();
    }

    @Test
    void shouldPostBodyAndHeadersOnSuccess() {
        statusSequence = new int[]{ 200 };
        assertThatCode(() -> client(true).post(url(), Map.of("Content-Type", "application/json"), "{\"k\":1}", 3))
                .doesNotThrowAnyException();
        assertThat(requestCount.get()).isEqualTo(1);
        assertThat(receivedMethods).containsExactly("POST");
        assertThat(receivedBodies).containsExactly("{\"k\":1}");
        assertThat(receivedContentTypes).containsExactly("application/json");
    }

    @Test
    void shouldUseTheGivenMethod() {
        statusSequence = new int[]{ 200 };
        assertThatCode(() -> client(true).send("PUT", url(), Map.of("Content-Type", "application/json"), "body", 1))
                .doesNotThrowAnyException();
        assertThat(receivedMethods).containsExactly("PUT");
    }

    @Test
    void shouldRetryThenSucceed() {
        statusSequence = new int[]{ 503, 503, 200 };
        assertThatCode(() -> client(true).post(url(), Map.of("Content-Type", "application/json"), "body", 3))
                .doesNotThrowAnyException();
        assertThat(requestCount.get()).isEqualTo(3);
    }

    @Test
    void shouldThrowAfterExhaustingRetries() {
        statusSequence = new int[]{ 500, 500, 500 };
        assertThatExceptionOfType(SsrfSafeHttpClient.HttpDeliveryException.class)
                .isThrownBy(() -> client(true).post(url(), Map.of("Content-Type", "application/json"), "body", 3));
        assertThat(requestCount.get()).isEqualTo(3);
    }

    @Test
    void shouldFailFastOnSsrfWithoutContactingServer() {
        statusSequence = new int[]{ 200 };
        assertThatExceptionOfType(SsrfSafeHttpClient.SsrfValidationException.class)
                .isThrownBy(() -> client(false).post(url(), Map.of("Content-Type", "application/json"), "body", 3));
        assertThat(requestCount.get()).isZero();
    }

    private final class RecordingHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            int index = requestCount.getAndIncrement();
            receivedMethods.add(exchange.getRequestMethod());
            receivedContentTypes.add(exchange.getRequestHeaders().getFirst("Content-Type"));
            try (InputStream in = exchange.getRequestBody()) {
                receivedBodies.add(new String(in.readAllBytes(), StandardCharsets.UTF_8));
            }
            int status = statusSequence[Math.min(index, statusSequence.length - 1)];
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        }
    }
}
