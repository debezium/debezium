/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;

import io.debezium.connector.mongodb.connection.MongoDbConnections;
import io.debezium.function.BlockingConsumer;

class MongoDbConnectionLifecycleTest {
    @Test
    void shouldReleaseOwnedContextAfterOperationCompletes() throws InterruptedException {
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = tracker.track(TestHelper.getConfiguration());
            try (var connection = MongoDbConnections.create(config)) {
                connection.execute("close during operation", client -> {
                    connection.close();
                    tracker.assertAuthenticationActive();
                });
            }
            tracker.assertClientsCreated();
            tracker.assertReleased();
        }
    }

    @Test
    void shouldCloseEveryRetryClient() throws InterruptedException {
        try (var tracker = new ConnectionResourceTracker()) {
            final var attempts = new AtomicInteger();
            final var config = tracker.track(TestHelper.getConfiguration());
            try (var connection = MongoDbConnections.create(config, (description, error) -> {
                assertThat(error).hasMessage("retry");
            })) {
                connection.execute("retry operation", client -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new IllegalStateException("retry");
                    }
                });
            }
            assertThat(attempts).hasValue(2);
            tracker.assertClientCount(2);
            tracker.assertReleased();
        }
    }

    @Test
    void shouldKeepAuthenticationAliveAcrossNestedOperations() throws InterruptedException {
        try (var tracker = new ConnectionResourceTracker()) {
            try (var connection = MongoDbConnections.create(tracker.track(TestHelper.getConfiguration()))) {
                connection.execute("outer", outer -> {
                    connection.execute("inner", inner -> {
                        tracker.assertAuthenticationActive();
                    });
                    tracker.assertAuthenticationActive();
                });
            }
            tracker.assertClientCount(2);
            tracker.assertReleased();
        }
    }

    @Test
    void shouldKeepBorrowedContextAliveUntilTaskStops() throws InterruptedException {
        try (var tracker = new ConnectionResourceTracker()) {
            try (var task = new MongoDbTaskContext(tracker.track(TestHelper.getConfiguration()))) {
                final var context = task.getConnectionContext();
                try (var connection = MongoDbConnections.create(context, null, null)) {
                    connection.execute("borrowed context", client -> {
                        tracker.assertAuthenticationActive();
                    });
                }
                tracker.assertAuthenticationActive();
                assertThat(task.getConnectionContext()).isSameAs(context);
            }
            tracker.assertReleased();
        }
    }

    @Test
    void shouldNotCreateClientsAfterTaskStops() {
        try (var tracker = new ConnectionResourceTracker()) {
            final var task = new MongoDbTaskContext(tracker.track(TestHelper.getConfiguration()));
            final var context = task.getConnectionContext();
            task.close();
            task.close();
            assertThatThrownBy(context::getMongoClient).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(task::getConnectionContext).isInstanceOf(IllegalStateException.class);
            tracker.assertReleased();
        }
    }

    @Test
    void shouldPreserveOperationFailureWhenDeferredCleanupFails() {
        try (var tracker = new ConnectionResourceTracker()) {
            tracker.closeFailure = new IllegalStateException("Cleanup failed");
            final var operationFailure = new IllegalStateException("Operation failed");
            try (var connection = MongoDbConnections.create(tracker.track(TestHelper.getConfiguration()))) {
                final BlockingConsumer<MongoClient> operation = client -> {
                    connection.close();
                    throw operationFailure;
                };
                assertThatThrownBy(() -> connection.execute("failed operation", operation))
                        .hasCause(operationFailure);
                assertThat(operationFailure).hasSuppressedException(tracker.closeFailure);
            }
            tracker.assertReleased();
        }
    }
}
