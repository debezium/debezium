/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e.source;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.WaitingConsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;

import okhttp3.OkHttpClient;
import okhttp3.Request;

class SourceStartupDiagnostics {

    static final int MAX_LOG_CHARACTERS = 16_384;
    private static final int QUERY_TIMEOUT_SECONDS = 5;
    private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .callTimeout(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS).build();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SourceType sourceType;
    private final String connectorName;
    private final Supplier<JsonNode> connectorStatus;
    private final Supplier<String> containerLogs;
    private final LogCapture subscription = new LogCapture();

    SourceStartupDiagnostics(SourceType sourceType, String connectorName, Supplier<JsonNode> connectorStatus,
                             Supplier<String> containerLogs) {
        this.sourceType = sourceType;
        this.connectorName = connectorName;
        this.connectorStatus = connectorStatus;
        this.containerLogs = containerLogs;
    }

    FrameConsumerResultCallback callback(WaitingConsumer wait) {
        subscription.addConsumer(OutputFrame.OutputType.STDOUT, frame -> {
            subscription.accept(frame);
            wait.accept(frame);
        });
        return subscription;
    }

    IllegalStateException timeout(String message, TimeoutException cause) {
        final var timedOutAt = Instant.now();
        final var logs = subscription.output();
        final var termination = subscription.termination;
        final List<Throwable> diagnosticFailures = new ArrayList<>();
        if (termination != null && termination.error() != null) {
            diagnosticFailures.add(termination.error());
        }
        final var failure = new IllegalStateException("Failed to wait for '" + message + "'"
                + " (source=" + sourceType + ", connector=" + connectorName + ")"
                + "\nStartup wait timed out at: " + timedOutAt
                + "\nLog subscription: " + (termination == null ? "still open at timeout" : termination)
                + "\n" + query(() -> formatStatus(connectorStatus.get()), diagnosticFailures)
                + "\nRecent subscribed Connect output (up to " + MAX_LOG_CHARACTERS + " characters):\n" + logs
                + "\nIndependently fetched Connect output (up to " + MAX_LOG_CHARACTERS + " characters):\n"
                + query(() -> bounded(containerLogs.get()), diagnosticFailures), cause);
        diagnosticFailures.forEach(failure::addSuppressed);
        return failure;
    }

    static JsonNode readStatus(String uri) {
        final var request = new Request.Builder().url(uri).build();
        try (var response = CLIENT.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IllegalStateException("Connect status request returned HTTP " + response.code());
            }
            return MAPPER.readTree(response.body().string());
        }
        catch (IOException e) {
            throw new IllegalStateException("Could not read Connect status", e);
        }
    }

    static String readLogs(DockerClient client, String containerId, int since) {
        try (var capture = new LogCapture(); var command = client.logContainerCmd(containerId)) {
            command.withFollowStream(false).withTail(200).withSince(since).withTimestamps(true)
                    .withStdOut(true).withStdErr(true).exec(capture);
            if (!capture.awaitCompletion(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Connect log query did not complete within " + QUERY_TIMEOUT_SECONDS + " seconds");
            }
            final var termination = capture.termination;
            if (termination != null && termination.error() != null) {
                throw new IllegalStateException("Could not read Connect logs", termination.error());
            }
            return capture.output();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading Connect logs", e);
        }
        catch (IOException e) {
            throw new IllegalStateException("Could not close Connect log query", e);
        }
    }

    private static String formatStatus(JsonNode status) {
        if (status == null || status.isNull()) {
            return "Connector status: <not available>";
        }
        final var result = new StringBuilder("Connector state: ")
                .append(status.path("connector").path("state").asText("<not available>"));
        for (var task : status.path("tasks")) {
            result.append("\nTask ").append(task.path("id").asText("?"))
                    .append(" state: ").append(task.path("state").asText("<not available>"));
            if (task.hasNonNull("trace")) {
                result.append("\nTask ").append(task.path("id").asText("?"))
                        .append(" trace:\n").append(bounded(task.path("trace").asText()));
            }
        }
        return result.toString();
    }

    private static String query(Supplier<String> query, List<Throwable> failures) {
        try {
            return query.get();
        }
        catch (RuntimeException e) {
            failures.add(e);
            return "<unavailable: " + e + ">";
        }
    }

    private static String bounded(String output) {
        return output == null || output.isEmpty() ? "<no output received>"
                : output.substring(Math.max(0, output.length() - MAX_LOG_CHARACTERS));
    }

    private record Termination(Instant at, Throwable error) {
        @Override
        public String toString() {
            return error == null ? "completed at " + at : "failed at " + at + ": " + error;
        }
    }

    private static class LogCapture extends FrameConsumerResultCallback {

        private final StringBuilder recentLogs = new StringBuilder();
        private volatile Termination termination;
        private boolean closed;

        LogCapture() {
            addConsumer(OutputFrame.OutputType.STDOUT, this::accept);
            addConsumer(OutputFrame.OutputType.STDERR, this::accept);
        }

        private synchronized void accept(OutputFrame frame) {
            if (frame.getType() == OutputFrame.OutputType.END) {
                return;
            }
            final var output = frame.getUtf8String();
            final int start = Math.max(0, output.length() - MAX_LOG_CHARACTERS);
            final int retainedLength = output.length() - start;
            recentLogs.delete(0, Math.max(0, recentLogs.length() + retainedLength - MAX_LOG_CHARACTERS));
            recentLogs.append(output, start, output.length());
        }

        private synchronized String output() {
            return bounded(recentLogs.toString());
        }

        @Override
        public synchronized void onStart(Closeable stream) {
            if (closed) {
                try {
                    stream.close();
                }
                catch (IOException e) {
                    recordTermination(e);
                }
                return;
            }
            super.onStart(stream);
        }

        @Override
        public synchronized void close() throws IOException {
            closed = true;
            super.close();
        }

        @Override
        public void onError(Throwable error) {
            recordTermination(error);
            super.onError(error);
        }

        @Override
        public void onComplete() {
            recordTermination(null);
            super.onComplete();
        }

        private synchronized void recordTermination(Throwable error) {
            if (termination == null) {
                termination = new Termination(Instant.now(), error);
            }
        }
    }
}
