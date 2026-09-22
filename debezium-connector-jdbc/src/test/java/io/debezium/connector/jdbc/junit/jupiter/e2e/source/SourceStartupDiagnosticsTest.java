/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.dockerclient.DockerClientProviderStrategy;
import org.testcontainers.dockerclient.TransportConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.debezium.doc.FixFor;

@Tag("UnitTests")
class SourceStartupDiagnosticsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void shouldKeepSuccessfulStartupDetectionWithoutQueryingDiagnostics() throws Exception {
        final var queries = new AtomicInteger();
        final var diagnostics = new SourceStartupDiagnostics(SourceType.POSTGRES, "jdbc-source-1", () -> {
            queries.incrementAndGet();
            return status();
        }, () -> {
            queries.incrementAndGet();
            return "not queried";
        });
        final var wait = new WaitingConsumer();
        try (var callback = diagnostics.callback(wait)) {
            callback.onNext(frame(StreamType.STDERR, "Starting streaming on stderr\n"));
            assertThat(wait.getFrames()).isEmpty();
            callback.onNext(frame(StreamType.STDOUT, "Starting "));
            callback.onNext(frame(StreamType.STDOUT, "streaming\n"));
            wait.waitUntil(output -> output.getUtf8String().contains("Starting streaming"), 1, TimeUnit.SECONDS);
        }
        assertThat(queries).hasValue(0);
    }

    @Test
    @FixFor("debezium/dbz#2684")
    void shouldReportTaskTraceAndIndependentLogsAfterSubscriptionFailure() throws Exception {
        final var streamFailure = new SocketTimeoutException("Docker stream interrupted");
        final var diagnostics = new SourceStartupDiagnostics(SourceType.SQLSERVER, "jdbc-source-42",
                SourceStartupDiagnosticsTest::status, () -> "Starting streaming\n");
        final var timeout = new TimeoutException();
        try (var callback = diagnostics.callback(new WaitingConsumer())) {
            callback.onStart(() -> {
            });
            assertThat(callback.awaitStarted(1, TimeUnit.SECONDS)).isTrue();
            callback.onNext(frame(StreamType.STDOUT, "Starting connector\n"));
            callback.onNext(frame(StreamType.STDERR, "Could not connect to database\n"));
            callback.onError(streamFailure);
            callback.onError(new IOException("Secondary close error"));
            final var failure = diagnostics.timeout("Starting streaming", timeout);
            assertThat(failure).hasMessageContainingAll("Failed to wait for 'Starting streaming'",
                    "source=SQLSERVER", "connector=jdbc-source-42", "Connector state: RUNNING", "Task 0 state: FAILED",
                    "Task 0 trace:\nDatabase unavailable\nCaused by: connection refused", "failed at",
                    "Docker stream interrupted", "Starting connector\nCould not connect to database\n",
                    "Independently fetched Connect output", "Starting streaming\n");
            assertThat(failure.getCause()).isSameAs(timeout);
            assertThat(failure.getSuppressed()).containsExactly(streamFailure);
        }
    }

    @Test
    void shouldReportCompletedSubscriptionSeparatelyFromOpenSubscription() throws Exception {
        final var diagnostics = new SourceStartupDiagnostics(SourceType.POSTGRES, "jdbc-source-1", () -> null, () -> null);
        try (var callback = diagnostics.callback(new WaitingConsumer())) {
            assertThat(diagnostics.timeout("Starting streaming", new TimeoutException()))
                    .hasMessageContaining("Log subscription: still open at timeout");
            callback.onComplete();
            final var failure = diagnostics.timeout("Starting streaming", new TimeoutException());
            assertThat(failure).hasMessageContaining("Log subscription: completed at");
            assertThat(failure.getSuppressed()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldPreserveOtherDiagnosticsWhenQueryFails(boolean statusQueryFails) throws Exception {
        final var queryFailure = new IllegalStateException("Diagnostic request unavailable");
        final var diagnostics = new SourceStartupDiagnostics(SourceType.SQLSERVER, "jdbc-source-42", () -> {
            if (statusQueryFails) {
                throw queryFailure;
            }
            return status();
        }, () -> {
            if (!statusQueryFails) {
                throw queryFailure;
            }
            return "Independently retrieved log\n";
        });
        try (var callback = diagnostics.callback(new WaitingConsumer())) {
            callback.onNext(frame(StreamType.STDERR, "Database error\n"));
            final var timeout = new TimeoutException();
            final var failure = diagnostics.timeout("Starting streaming", timeout);
            assertThat(failure.getCause()).isSameAs(timeout);
            assertThat(failure.getSuppressed()).containsExactly(queryFailure);
            assertThat(failure).hasMessageContainingAll("Diagnostic request unavailable", "Database error",
                    statusQueryFails ? "Independently retrieved log" : "Task 0 trace:\nDatabase unavailable");
        }
    }

    @Test
    void shouldPreserveAllFailuresWithoutReplacingTimeout() throws Exception {
        final var streamFailure = new IOException("Stream failed");
        final var statusFailure = new IllegalStateException("Status failed");
        final var logsFailure = new IllegalStateException("Logs failed");
        final var diagnostics = new SourceStartupDiagnostics(SourceType.SQLSERVER, "jdbc-source-42", () -> {
            throw statusFailure;
        }, () -> {
            throw logsFailure;
        });
        try (var callback = diagnostics.callback(new WaitingConsumer())) {
            callback.onError(streamFailure);
            final var timeout = new TimeoutException();
            final var failure = diagnostics.timeout("Starting streaming", timeout);
            assertThat(failure.getCause()).isSameAs(timeout);
            assertThat(failure.getSuppressed()).containsExactly(streamFailure, statusFailure, logsFailure);
        }
    }

    @Test
    void shouldReportMissingStatusAndEmptyOutput() {
        final var diagnostics = new SourceStartupDiagnostics(SourceType.POSTGRES, "jdbc-source-1", () -> null, () -> null);
        assertThat(diagnostics.timeout("Starting streaming", new TimeoutException()))
                .hasMessageContainingAll("Connector status: <not available>", "<no output received>");
    }

    @Test
    void shouldCloseStreamArrivingAfterCallbackWasClosed() throws Exception {
        final var diagnostics = new SourceStartupDiagnostics(SourceType.POSTGRES, "jdbc-source-1", () -> null, () -> null);
        final var streamClosed = new AtomicBoolean();
        try (var callback = diagnostics.callback(new WaitingConsumer())) {
            callback.close();
            callback.onStart(() -> streamClosed.set(true));
            assertThat(streamClosed).isTrue();
        }
    }

    @Test
    void shouldRetainOnlyBoundedOutputForBothLogSources() throws Exception {
        final var diagnostics = new SourceStartupDiagnostics(SourceType.POSTGRES, "jdbc-source-1", () -> null,
                () -> "y".repeat(SourceStartupDiagnostics.MAX_LOG_CHARACTERS * 2) + "fetched tail\n");
        try (var callback = diagnostics.callback(new WaitingConsumer())) {
            callback.onNext(frame(StreamType.STDOUT, "old output\n"));
            callback.onNext(frame(StreamType.STDOUT, "x".repeat(SourceStartupDiagnostics.MAX_LOG_CHARACTERS * 2) + "\n"));
            callback.onNext(frame(StreamType.STDERR, "latest error\n"));
            final var message = diagnostics.timeout("Starting streaming", new TimeoutException()).getMessage();
            final var sections = message.split("characters\\):\n");
            final var subscribed = sections[1].substring(0, sections[1].indexOf("\nIndependently fetched"));
            assertThat(subscribed).hasSize(SourceStartupDiagnostics.MAX_LOG_CHARACTERS)
                    .doesNotContain("old output").endsWith("latest error\n");
            assertThat(sections[2]).hasSize(SourceStartupDiagnostics.MAX_LOG_CHARACTERS).endsWith("fetched tail\n");
        }
    }

    @Test
    void shouldReadStatusAndTraceFromRestEndpoint() throws Exception {
        try (var endpoint = new Endpoint(exchange -> respond(exchange, 200, status().toString().getBytes(StandardCharsets.UTF_8)))) {
            assertThat(SourceStartupDiagnostics.readStatus(endpoint.uri("/connectors/jdbc-source-42/status").toString()))
                    .isEqualTo(status());
        }
    }

    @Test
    void shouldReportFailedStatusRequest() throws Exception {
        try (var endpoint = new Endpoint(exchange -> respond(exchange, 503, new byte[0]))) {
            assertThatThrownBy(() -> SourceStartupDiagnostics.readStatus(endpoint.uri("/connectors/source/status").toString()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("HTTP 503");
        }
    }

    @Test
    void shouldFetchDockerLogsIndependentlyWithTimeAndSizeFilters() throws Exception {
        final var request = new AtomicReference<URI>();
        try (var endpoint = new Endpoint(exchange -> {
            request.set(exchange.getRequestURI());
            final byte[] log = "Starting streaming\n".getBytes(StandardCharsets.UTF_8);
            final var frame = ByteBuffer.allocate(8 + log.length).put((byte) 1).put(new byte[3]).putInt(log.length).put(log).array();
            respond(exchange, 200, frame);
        }); var client = endpoint.dockerClient()) {
            assertThat(SourceStartupDiagnostics.readLogs(client, "connect-container", 123)).isEqualTo("Starting streaming\n");
            assertThat(request.get().getPath()).endsWith("/containers/connect-container/logs");
            assertThat(request.get().getQuery()).contains("tail=200", "since=123", "stdout=true", "stderr=true", "timestamps=true")
                    .doesNotContain("follow=true");
        }
    }

    @Test
    void shouldPreserveDockerLogRequestFailure() throws Exception {
        try (var endpoint = new Endpoint(exchange -> respond(exchange, 500, "Docker unavailable".getBytes(StandardCharsets.UTF_8)));
                var client = endpoint.dockerClient()) {
            assertThatThrownBy(() -> SourceStartupDiagnostics.readLogs(client, "connect-container", 123))
                    .isInstanceOf(IllegalStateException.class).hasMessage("Could not read Connect logs")
                    .hasRootCauseMessage("Status 500: Docker unavailable");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldBoundDiagnosticRequests(boolean statusRequest) throws Exception {
        final var release = new CountDownLatch(1);
        try (var endpoint = new Endpoint(exchange -> {
            try {
                release.await(15, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finally {
                exchange.close();
            }
        }); var client = endpoint.dockerClient()) {
            try {
                if (statusRequest) {
                    assertThatThrownBy(() -> SourceStartupDiagnostics.readStatus(endpoint.uri("/connectors/source/status").toString()))
                            .isInstanceOf(IllegalStateException.class).hasMessage("Could not read Connect status")
                            .hasCauseInstanceOf(InterruptedIOException.class);
                }
                else {
                    assertThatThrownBy(() -> SourceStartupDiagnostics.readLogs(client, "connect-container", 123))
                            .isInstanceOf(IllegalStateException.class).hasMessageContaining("did not complete within 5 seconds");
                }
            }
            finally {
                release.countDown();
            }
        }
    }

    private static JsonNode status() {
        final var status = MAPPER.createObjectNode();
        status.putObject("connector").put("state", "RUNNING");
        status.putArray("tasks").addObject().put("id", 0).put("state", "FAILED")
                .put("trace", "Database unavailable\nCaused by: connection refused");
        return status;
    }

    private static Frame frame(StreamType stream, String text) {
        return new Frame(stream, text.getBytes(StandardCharsets.UTF_8));
    }

    private static void respond(HttpExchange exchange, int code, byte[] body) throws IOException {
        try (exchange) {
            exchange.sendResponseHeaders(code, body.length == 0 ? -1 : body.length);
            exchange.getResponseBody().write(body);
        }
    }

    private static class Endpoint implements AutoCloseable {
        private final HttpServer server;

        Endpoint(HttpHandler handler) throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                if (exchange.getRequestURI().getPath().endsWith("/_ping")) {
                    respond(exchange, 200, "OK".getBytes(StandardCharsets.UTF_8));
                }
                else {
                    handler.handle(exchange);
                }
            });
            server.start();
        }

        URI uri(String path) {
            return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
        }

        DockerClient dockerClient() {
            return DockerClientProviderStrategy.getClientForConfig(TransportConfig.builder()
                    .dockerHost(URI.create("tcp://127.0.0.1:" + server.getAddress().getPort())).build());
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
