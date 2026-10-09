/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.doc.FixFor;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

class BaseSourceTaskShutdownTest {

    @Test
    @FixFor("debezium/dbz#2709")
    void shouldCleanUpAfterCoordinatorStops() {
        final var task = new ShutdownTask(() -> {
        });

        task.stop();

        assertThat(task.shutdownSteps).containsExactly("coordinator", "task");
        assertThat(task.coordinator).isNull();
        assertThat(task.getTaskState()).isEqualTo(DebeziumTaskState.STOPPED);
    }

    @Test
    @FixFor("debezium/dbz#2709")
    void shouldCleanUpWithoutCoordinator() {
        final var task = new ShutdownTask(() -> {
        });
        task.coordinator = null;

        task.stop();

        assertThat(task.shutdownSteps).containsExactly("task");
        assertThat(task.getTaskState()).isEqualTo(DebeziumTaskState.STOPPED);
    }

    @ParameterizedTest
    @MethodSource("uncheckedFailures")
    @FixFor("debezium/dbz#2709")
    void shouldCleanUpAfterCoordinatorFailure(Throwable failure) {
        final var task = new ShutdownTask(() -> throwUnchecked(failure));

        assertThat(catchThrowable(task::stop)).isSameAs(failure);
        assertThat(task.shutdownSteps).containsExactly("coordinator", "task");
        assertThat(failure.getSuppressed()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("uncheckedFailures")
    @FixFor("debezium/dbz#2709")
    void shouldPreserveCoordinatorFailureWhenCleanupFails(Throwable failure) {
        final var task = new ShutdownTask(() -> throwUnchecked(failure));
        final var cleanupFailure = new IllegalStateException("Cleanup failed");
        task.cleanup = () -> {
            throw cleanupFailure;
        };

        assertThat(catchThrowable(task::stop)).isSameAs(failure);
        assertThat(task.shutdownSteps).containsExactly("coordinator", "task");
        assertThat(failure.getSuppressed()).containsExactly(cleanupFailure);
    }

    @Test
    @FixFor("debezium/dbz#2709")
    void shouldNotSuppressFailureOntoItself() {
        final var failure = new IllegalStateException("Shared failure");
        final var task = new ShutdownTask(() -> {
            throw failure;
        });
        task.cleanup = () -> {
            throw failure;
        };

        assertThat(catchThrowable(task::stop)).isSameAs(failure);
        assertThat(task.shutdownSteps).containsExactly("coordinator", "task");
        assertThat(failure.getSuppressed()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    @FixFor("debezium/dbz#2709")
    void shouldCleanUpAfterCoordinatorInterruption(boolean cleanupFails) {
        final var failure = new InterruptedException("Coordinator interrupted");
        final var cleanupFailure = new IllegalStateException("Cleanup failed");
        final var task = new ShutdownTask(() -> {
            throw failure;
        });
        task.cleanup = () -> {
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
            if (cleanupFails) {
                throw cleanupFailure;
            }
        };

        try {
            final var thrown = catchThrowable(task::stop);

            assertThat(task.shutdownSteps).containsExactly("coordinator", "task");
            assertThat(thrown).isInstanceOf(ConnectException.class).hasCause(failure);
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
            if (cleanupFails) {
                assertThat(thrown.getSuppressed()).containsExactly(cleanupFailure);
            }
            else {
                assertThat(thrown.getSuppressed()).isEmpty();
            }
        }
        finally {
            Thread.interrupted();
        }
    }

    @ParameterizedTest
    @MethodSource("uncheckedFailures")
    @FixFor("debezium/dbz#2709")
    void shouldPropagateCleanupFailure(Throwable failure) {
        final var task = new ShutdownTask(() -> {
        });
        task.cleanup = () -> throwUnchecked(failure);

        assertThat(catchThrowable(task::stop)).isSameAs(failure);
        assertThat(task.shutdownSteps).containsExactly("coordinator", "task");
    }

    @Test
    @FixFor("debezium/dbz#2709")
    void shouldRestartAfterSuccessfulCleanup() throws InterruptedException {
        final var task = new ShutdownTask(() -> {
        });
        final var failure = new RetriableException("Polling failed");
        task.pollFailure = failure;

        assertThat(catchThrowable(task::poll)).isSameAs(failure);
        assertThat(task.shutdownSteps).containsExactly("coordinator", "task");
        assertThat(task.getTaskState()).isEqualTo(DebeziumTaskState.RESTARTING);

        task.pollFailure = null;
        Thread.sleep(1);
        task.poll();

        assertThat(task.startCount).isEqualTo(2);
        assertThat(task.getTaskState()).isEqualTo(DebeziumTaskState.RUNNING);
        task.stop();
        assertThat(task.shutdownSteps).containsExactly("coordinator", "task", "coordinator", "task");
    }

    @ParameterizedTest
    @MethodSource("uncheckedFailures")
    @FixFor("debezium/dbz#2709")
    void shouldCleanUpWhenCoordinatorFailsDuringRestart(Throwable failure) {
        final var task = new ShutdownTask(() -> throwUnchecked(failure));
        task.pollFailure = new RetriableException("Polling failed");

        assertThat(catchThrowable(task::poll)).isSameAs(failure);
        assertThat(task.shutdownSteps).containsExactly("coordinator", "task");
        assertThat(task.startCount).isEqualTo(1);
        assertThat(task.getTaskState()).isEqualTo(DebeziumTaskState.RUNNING);
    }

    private static Stream<Throwable> uncheckedFailures() {
        return Stream.of(new IllegalStateException("Shutdown failed"), new AssertionError("Shutdown failed"));
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException exception) {
            throw exception;
        }
        throw (Error) failure;
    }

    @FunctionalInterface
    private interface CoordinatorShutdown {
        void stop() throws InterruptedException;
    }

    private static class ShutdownTask extends BaseSourceTask<Partition, OffsetContext> {
        private final List<String> shutdownSteps = new ArrayList<>();
        private final CoordinatorShutdown shutdown;
        private Runnable cleanup = () -> {
        };
        private RetriableException pollFailure;
        private int startCount;

        private ShutdownTask(CoordinatorShutdown shutdown) {
            this.shutdown = shutdown;
            final Map<String, String> properties = Map.of(
                    CommonConnectorConfig.TOPIC_PREFIX.name(), "shutdown-test",
                    CommonConnectorConfig.RETRIABLE_RESTART_WAIT.name(), "1");
            initialize(new SourceTaskContext() {
                @Override
                public Map<String, String> configs() {
                    return properties;
                }

                @Override
                public OffsetStorageReader offsetStorageReader() {
                    return null;
                }

                @Override
                public PluginMetrics pluginMetrics() {
                    return null;
                }
            });
            start(properties);
        }

        @Override
        public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {
            return new CdcSourceTaskContext<>(config, new TestRelationalDatabaseConfig(config, null, null, 1), "0", Map.of());
        }

        @Override
        protected ChangeEventSourceCoordinator<Partition, OffsetContext> start(Configuration config) {
            startCount++;
            final var connectorConfig = new TestRelationalDatabaseConfig(config, null, null, 1);
            // No worker is started; only the coordinator shutdown boundary is exercised.
            return new ChangeEventSourceCoordinator<>(null, null, SourceConnector.class, connectorConfig,
                    null, null, null, null, null, null, null) {
                @Override
                public void stop() throws InterruptedException {
                    shutdownSteps.add("coordinator");
                    shutdown.stop();
                }
            };
        }

        @Override
        protected String connectorName() {
            return "shutdown-test";
        }

        @Override
        protected List<SourceRecord> doPoll() {
            if (pollFailure != null) {
                throw pollFailure;
            }
            return List.of();
        }

        @Override
        protected Optional<ErrorHandler> getErrorHandler() {
            return Optional.empty();
        }

        @Override
        protected void doStop() {
            shutdownSteps.add("task");
            cleanup.run();
        }

        @Override
        protected Iterable<Field> getAllConfigurationFields() {
            return List.of();
        }

        @Override
        public String version() {
            return "1.0";
        }
    }
}
