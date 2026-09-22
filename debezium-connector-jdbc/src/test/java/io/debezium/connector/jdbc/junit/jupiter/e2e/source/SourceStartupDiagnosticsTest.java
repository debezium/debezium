/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.WaitingConsumer;

import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;

import io.debezium.doc.FixFor;
import io.debezium.testing.testcontainers.Connector;

@Tag("UnitTests")
class SourceStartupDiagnosticsTest {

    @Test
    void shouldKeepSuccessfulStartupDetectionWithoutQueryingStates() throws Exception {
        final var queries = new AtomicInteger();
        final var diagnostics = new SourceStartupDiagnostics(SourceType.POSTGRES, "jdbc-source-1", () -> {
            queries.incrementAndGet();
            return Connector.State.RUNNING;
        }, () -> {
            queries.incrementAndGet();
            return Connector.State.RUNNING;
        });
        final var wait = new WaitingConsumer();
        try (var callback = new FrameConsumerResultCallback()) {
            diagnostics.attachTo(callback, wait);
            callback.onNext(frame(StreamType.STDERR, "Starting streaming on stderr\n"));
            assertThat(wait.getFrames()).isEmpty();
            callback.onNext(frame(StreamType.STDOUT, "Starting streaming\n"));
            wait.waitUntil(output -> output.getUtf8String().contains("Starting streaming"), 1, TimeUnit.SECONDS);
        }
        assertThat(queries).hasValue(0);
    }

    @Test
    @FixFor("debezium/dbz#2684")
    void shouldReportSourceStatesAndBothOutputStreams() throws Exception {
        final var diagnostics = new SourceStartupDiagnostics(SourceType.SQLSERVER, "jdbc-source-42",
                () -> Connector.State.RUNNING, () -> Connector.State.FAILED);
        try (var callback = new FrameConsumerResultCallback()) {
            diagnostics.attachTo(callback, new WaitingConsumer());
            callback.onNext(frame(StreamType.STDOUT, "Starting connector\n"));
            callback.onNext(frame(StreamType.STDERR, "Could not connect to database\n"));
        }
        final var timeout = new TimeoutException();
        final var failure = diagnostics.timeout("Starting streaming", timeout);
        assertThat(failure).hasMessageContainingAll("Failed to wait for 'Starting streaming'",
                "source=SQLSERVER", "connector=jdbc-source-42", "Connector state: RUNNING", "Task 0 state: FAILED",
                "Starting connector\nCould not connect to database\n");
        assertThat(failure.getCause()).isSameAs(timeout);
        assertThat(failure.getSuppressed()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldPreserveTimeoutAndOtherDiagnosticsWhenStateQueryFails(boolean connectorQueryFails) {
        final var queryFailure = new IllegalStateException("Connect REST unavailable");
        final var diagnostics = new SourceStartupDiagnostics(SourceType.SQLSERVER, "jdbc-source-42", () -> {
            if (connectorQueryFails) {
                throw queryFailure;
            }
            return Connector.State.RUNNING;
        }, () -> {
            if (!connectorQueryFails) {
                throw queryFailure;
            }
            return Connector.State.FAILED;
        });
        diagnostics.accept(new OutputFrame(OutputFrame.OutputType.STDERR, "Database error\n".getBytes(StandardCharsets.UTF_8)));
        final var timeout = new TimeoutException();
        final var failure = diagnostics.timeout("Starting streaming", timeout);
        assertThat(failure.getCause()).isSameAs(timeout);
        assertThat(failure.getSuppressed()).containsExactly(queryFailure);
        assertThat(failure).hasMessageContainingAll("Connect REST unavailable", "Database error",
                connectorQueryFails ? "Task 0 state: FAILED" : "Connector state: RUNNING");
    }

    @Test
    void shouldReportMissingStatesAndEmptyOutput() {
        final var diagnostics = new SourceStartupDiagnostics(SourceType.POSTGRES, "jdbc-source-1", () -> null, () -> null);
        diagnostics.accept(OutputFrame.END);
        assertThat(diagnostics.timeout("Starting streaming", new TimeoutException()))
                .hasMessageContainingAll("Connector state: <not available>", "Task 0 state: <not available>", "<no output received>");
    }

    @Test
    void shouldRetainOnlyBoundedRecentOutputEvenForOversizedFrames() {
        final var diagnostics = new SourceStartupDiagnostics(SourceType.POSTGRES, "jdbc-source-1", () -> null, () -> null);
        diagnostics.accept(new OutputFrame(OutputFrame.OutputType.STDOUT, "old output\n".getBytes(StandardCharsets.UTF_8)));
        diagnostics.accept(new OutputFrame(OutputFrame.OutputType.STDOUT,
                "x".repeat(SourceStartupDiagnostics.MAX_LOG_CHARACTERS * 2).getBytes(StandardCharsets.UTF_8)));
        diagnostics.accept(new OutputFrame(OutputFrame.OutputType.STDERR, "latest error\n".getBytes(StandardCharsets.UTF_8)));
        final var message = diagnostics.timeout("Starting streaming", new TimeoutException()).getMessage();
        final var logs = message.substring(message.indexOf("characters):\n") + "characters):\n".length());
        assertThat(logs).hasSize(SourceStartupDiagnostics.MAX_LOG_CHARACTERS)
                .doesNotContain("old output").endsWith("latest error\n");
    }

    private static Frame frame(StreamType stream, String text) {
        return new Frame(stream, text.getBytes(StandardCharsets.UTF_8));
    }
}
