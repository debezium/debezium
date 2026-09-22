/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e.source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.WaitingConsumer;

import io.debezium.testing.testcontainers.Connector;

class SourceStartupDiagnostics implements Consumer<OutputFrame> {

    static final int MAX_LOG_CHARACTERS = 16_384;

    private final SourceType sourceType;
    private final String connectorName;
    private final Supplier<Connector.State> connectorState;
    private final Supplier<Connector.State> taskState;
    private final StringBuilder recentLogs = new StringBuilder();

    SourceStartupDiagnostics(SourceType sourceType, String connectorName, Supplier<Connector.State> connectorState,
                             Supplier<Connector.State> taskState) {
        this.sourceType = sourceType;
        this.connectorName = connectorName;
        this.connectorState = connectorState;
        this.taskState = taskState;
    }

    void attachTo(FrameConsumerResultCallback callback, WaitingConsumer wait) {
        callback.addConsumer(OutputFrame.OutputType.STDOUT, andThen(wait));
        callback.addConsumer(OutputFrame.OutputType.STDERR, this);
    }

    @Override
    public synchronized void accept(OutputFrame frame) {
        if (frame.getType() == OutputFrame.OutputType.END) {
            return;
        }
        final var output = frame.getUtf8String();
        final int start = Math.max(0, output.length() - MAX_LOG_CHARACTERS);
        final int retainedLength = output.length() - start;
        recentLogs.delete(0, Math.max(0, recentLogs.length() + retainedLength - MAX_LOG_CHARACTERS));
        recentLogs.append(output, start, output.length());
    }

    IllegalStateException timeout(String message, TimeoutException cause) {
        final var logs = recentLogs();
        final List<RuntimeException> diagnosticFailures = new ArrayList<>();
        final var failure = new IllegalStateException("Failed to wait for '" + message + "'"
                + " (source=" + sourceType + ", connector=" + connectorName + ")"
                + "\nConnector state: " + state(connectorState, diagnosticFailures)
                + "\nTask 0 state: " + state(taskState, diagnosticFailures)
                + "\nRecent Connect output (up to " + MAX_LOG_CHARACTERS + " characters):\n" + logs, cause);
        diagnosticFailures.forEach(failure::addSuppressed);
        return failure;
    }

    private synchronized String recentLogs() {
        return recentLogs.isEmpty() ? "<no output received>" : recentLogs.toString();
    }

    private static String state(Supplier<Connector.State> state, List<RuntimeException> diagnosticFailures) {
        try {
            final var value = state.get();
            return value == null ? "<not available>" : value.name();
        }
        catch (RuntimeException e) {
            diagnosticFailures.add(e);
            return "<unavailable: " + e + ">";
        }
    }
}
