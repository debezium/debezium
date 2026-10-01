/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.codecs.DecoderContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor.EventFetcher;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor.EventFetcher.State;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor.ResumableChangeStreamEvent;
import io.debezium.function.ThrowingRunnable;
import io.debezium.util.Clock;

class BufferingChangeStreamCursorTest {

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);
    // These timeouts only bound a hung test; elapsed time is never an assertion.
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldWakeAWaitingConsumerWhenAnEventArrives(boolean document) throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL);
                var waiting = new AsyncOperation<>(() -> {
                    // Keep the wait pending until an explicit event, close, or interrupt.
                    fixture.fetcher.awaitEvent(Long.MAX_VALUE);
                    return fixture.cursor.tryNext();
                })) {
            final var event = event(1, document);
            fixture.whenWaiting(fixture.eventAvailable, () -> assertThat(fixture.enqueue(event)).isTrue());

            assertThat(waiting.get()).isSameAs(event);
            assertThat(fixture.cursor.getResumeToken()).isEqualTo(event.resumeToken);
        }
    }

    @Test
    void shouldWaitForEventsBeforeTheFetcherHasStarted() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            assertThat(fixture.fetcher.isRunning()).isFalse();
            final var event = event(1, true);
            // Stop at lock acquisition, before any short polling timeout can start.
            fixture.lock.lock();
            try (var polling = new AsyncOperation<>(fixture.cursor::tryNextInterruptibly)) {
                try {
                    fixture.awaitBlockedOnWaitLock(polling.thread);
                    assertThat(fixture.enqueue(event)).isTrue();
                }
                finally {
                    fixture.lock.unlock();
                }

                assertThat(polling.get()).isSameAs(event);
                assertThat(fixture.cursor.getResumeToken()).isEqualTo(event.resumeToken);
            }
        }
    }

    @Test
    void shouldPreserveEventOrderAndResumeTokens() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            final var events = List.of(event(1, true), event(2, false), event(3, true));
            for (final var event : events) {
                assertThat(fixture.enqueue(event)).isTrue();
            }

            assertThat(fixture.cursor.getResumeToken()).isNull();
            assertThat(fixture.cursor.hasNext()).isTrue();
            assertThat(fixture.cursor.available()).isEqualTo(events.size());
            for (final var event : events) {
                assertThat(fixture.cursor.tryNext()).isSameAs(event);
                assertThat(fixture.cursor.getResumeToken()).isEqualTo(event.resumeToken);
            }
            assertThat(fixture.cursor.available()).isZero();
            assertThat(fixture.cursor.hasNext()).isFalse();
        }
    }

    @Test
    void shouldReleaseBufferCapacityAfterConsumingAnEvent() throws Exception {
        try (var fixture = new CursorFixture(1, POLL_INTERVAL)) {
            final var first = event(1, true);
            final var second = event(2, false);
            assertThat(fixture.enqueue(first)).isTrue();
            assertThat(fixture.enqueue(second)).isFalse();
            assertThat(fixture.cursor.available()).isEqualTo(1);

            assertThat(fixture.cursor.tryNext()).isSameAs(first);
            assertThat(fixture.enqueue(second)).isTrue();
            assertThat(fixture.cursor.tryNext()).isSameAs(second);
            assertThat(fixture.cursor.available()).isZero();
        }
    }

    @Test
    void shouldReturnNullForAnEmptyBuffer() throws Exception {
        try (var fixture = new CursorFixture(4, Duration.ofMillis(20));
                var polling = new AsyncOperation<>(fixture.cursor::tryNext)) {
            assertThat(polling.get()).isNull();
            assertThat(fixture.cursor.getResumeToken()).isNull();
        }
    }

    @Test
    void shouldRetainLastConsumedResumeTokenOnAnEmptyPoll() throws Exception {
        try (var fixture = new CursorFixture(4, Duration.ofMillis(2))) {
            final var event = event(1, false);
            assertThat(fixture.enqueue(event)).isTrue();
            assertThat(fixture.cursor.tryNext()).isSameAs(event);

            assertThat(fixture.cursor.tryNext()).isNull();
            assertThat(fixture.cursor.getResumeToken()).isEqualTo(event.resumeToken);
        }
    }

    @Test
    void shouldDrainBufferedEventsBeforeReportingFetcherFailure() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            final var events = List.of(event(1, true), event(2, false));
            for (final var event : events) {
                assertThat(fixture.enqueue(event)).isTrue();
            }
            final var failure = new IllegalStateException("Change stream failed");
            fixture.fail(failure);

            for (final var event : events) {
                assertThat(fixture.cursor.tryNext()).isSameAs(event);
                assertThat(fixture.cursor.getResumeToken()).isEqualTo(event.resumeToken);
            }
            assertThatThrownBy(fixture.cursor::tryNext)
                    .isInstanceOf(DebeziumException.class)
                    .hasCause(failure);
        }
    }

    @Test
    void shouldWakeAWaitingConsumerWhenTheFetcherFails() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL);
                var waiting = new AsyncOperation<>(() -> {
                    fixture.fetcher.awaitEvent(Long.MAX_VALUE);
                    return fixture.cursor.tryNext();
                })) {
            final var failure = new IllegalStateException("Change stream failed");
            fixture.whenWaiting(fixture.eventAvailable, () -> fixture.fail(failure));

            assertThatThrownBy(waiting::get)
                    .hasCauseInstanceOf(DebeziumException.class)
                    .hasRootCause(failure);
        }
    }

    @Test
    void shouldWakeAWaitingConsumerWhenClosed() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL);
                var waiting = new AsyncOperation<>(() -> {
                    fixture.fetcher.awaitEvent(Long.MAX_VALUE);
                    return fixture.cursor.tryNext();
                })) {
            fixture.whenWaiting(fixture.eventAvailable, fixture.cursor::close);

            assertThat(waiting.get()).isNull();
        }
    }

    @Test
    void shouldPreserveInterruptStatusWhenAcquiringTheWaitLockIsInterrupted() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            fixture.lock.lock();
            try (var waiting = new AsyncOperation<>(() -> {
                assertThatThrownBy(fixture.cursor::tryNext)
                        .isInstanceOf(DebeziumException.class)
                        .hasCauseInstanceOf(InterruptedException.class);
                return Thread.currentThread().isInterrupted();
            })) {
                try {
                    fixture.awaitBlockedOnWaitLock(waiting.thread);
                    waiting.thread.interrupt();

                    assertThat(waiting.get()).isTrue();
                }
                finally {
                    fixture.lock.unlock();
                }
            }
        }
    }

    @Test
    void shouldPropagateInterruptionToTheStreamingCaller() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            fixture.lock.lock();
            try (var waiting = new AsyncOperation<>(fixture.cursor::tryNextInterruptibly)) {
                try {
                    fixture.awaitBlockedOnWaitLock(waiting.thread);
                    waiting.thread.interrupt();

                    assertThatThrownBy(waiting::get)
                            .hasCauseInstanceOf(InterruptedException.class);
                }
                finally {
                    fixture.lock.unlock();
                }
            }
        }
    }

    @Test
    void shouldNotOpenACursorWhenClosedBeforeTheFetcherStarts() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            fixture.cursor.close();

            // The fixture has no stream, so reaching stream.cursor() would publish
            // a failure. A closed fetcher must not attempt to open it.
            fixture.fetcher.run();

            assertThat(fixture.fetcher.isRunning()).isFalse();
            assertThat(fixture.fetcher.hasError()).isFalse();
            assertThat(fixture.cursor.getCurrentServerAddress()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldPreserveLifecycleWhenPausingAndResuming(boolean running) throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            if (running) {
                fixture.markFetcherRunning();
            }
            assertThat(fixture.fetcher.isRunning()).isEqualTo(running);
            assertThat(fixture.cursor.isPaused()).isFalse();

            fixture.cursor.pause();

            assertThat(fixture.fetcher.isRunning()).isEqualTo(running);
            assertThat(fixture.cursor.isPaused()).isTrue();

            fixture.cursor.resume();

            assertThat(fixture.fetcher.isRunning()).isEqualTo(running);
            assertThat(fixture.cursor.isPaused()).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRemainClosedAfterRepeatedClosePauseAndResume(boolean running) throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            if (running) {
                fixture.markFetcherRunning();
            }
            fixture.cursor.pause();
            fixture.cursor.close();

            assertThat(fixture.fetcher.isRunning()).isFalse();
            assertThat(fixture.cursor.isPaused()).isTrue();

            fixture.cursor.close();
            fixture.cursor.resume();
            fixture.cursor.pause();

            assertThat(fixture.fetcher.isRunning()).isFalse();
            assertThat(fixture.cursor.isPaused()).isTrue();

            fixture.cursor.resume();
            fixture.fetcher.run();

            assertThat(fixture.fetcher.isRunning()).isFalse();
            assertThat(fixture.cursor.isPaused()).isFalse();
            assertThat(fixture.fetcher.hasError()).isFalse();
            try (var waiting = new AsyncOperation<>(() -> {
                fixture.fetcher.awaitEvent(Long.MAX_VALUE);
                return fixture.cursor.tryNext();
            })) {
                assertThat(waiting.get()).isNull();
            }
        }
    }

    @Test
    void shouldDrainBufferedEventsAfterClosing() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            final var events = List.of(event(1, true), event(2, false));
            for (final var event : events) {
                assertThat(fixture.enqueue(event)).isTrue();
            }

            fixture.cursor.close();

            for (final var event : events) {
                assertThat(fixture.cursor.tryNext()).isSameAs(event);
                assertThat(fixture.cursor.getResumeToken()).isEqualTo(event.resumeToken);
            }
            assertThat(fixture.cursor.tryNext()).isNull();
            assertThat(fixture.cursor.getResumeToken()).isEqualTo(events.get(events.size() - 1).resumeToken);
        }
    }

    @Test
    void shouldObserveAnEventEnqueuedBeforeAcquiringTheWaitLock() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            // Hold the real condition lock so that the consumer cannot start waiting.
            // Enqueue before releasing it, when no condition signal can be retained.
            fixture.lock.lock();
            try (var waiting = new AsyncOperation<>(() -> {
                fixture.fetcher.awaitEvent(Long.MAX_VALUE);
                return fixture.cursor.tryNext();
            })) {
                final var event = event(1, true);
                try {
                    fixture.awaitBlockedOnWaitLock(waiting.thread);
                    assertThat(fixture.enqueue(event)).isTrue();
                }
                finally {
                    fixture.lock.unlock();
                }

                assertThat(waiting.get()).isSameAs(event);
            }
        }
    }

    @Test
    void shouldKeepWaitingAfterANotificationWithoutAnEvent() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL);
                var waiting = new AsyncOperation<>(() -> {
                    fixture.fetcher.awaitEvent(Long.MAX_VALUE);
                    return null;
                })) {
            fixture.whenWaiting(fixture.eventAvailable, () -> {
                fixture.eventAvailable.signalAll();
                // The original registration is gone. The next observation must see
                // the consumer register again after checking the empty queue.
                assertThat(fixture.lock.hasWaiters(fixture.eventAvailable)).isFalse();
            });
            final var event = event(1, true);
            fixture.whenWaiting(fixture.eventAvailable, () -> {
                assertThat(waiting.result).isNotDone();
                assertThat(fixture.enqueue(event)).isTrue();
            });

            assertThat(waiting.get()).isNull();
            assertThat(fixture.cursor.tryNext()).isSameAs(event);
        }
    }

    @Test
    void shouldDrainBufferedEventsWhileFetchingIsPaused() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            final var event = event(1, true);
            assertThat(fixture.enqueue(event)).isTrue();
            fixture.cursor.pause();

            assertThat(fixture.cursor.isPaused()).isTrue();
            assertThat(fixture.cursor.tryNext()).isSameAs(event);
            assertThat(fixture.cursor.isPaused()).isTrue();
        }
    }

    @Test
    void shouldResumeAPausedFetcher() throws Exception {
        try (var fixture = new CursorFixture(4, POLL_INTERVAL)) {
            fixture.cursor.pause();
            try (var paused = new AsyncOperation<>(() -> {
                fixture.fetcher.waitIfPaused();
                return fixture.fetcher.isPaused();
            })) {
                fixture.whenWaiting(fixture.resumed, fixture.cursor::resume);

                assertThat(paused.get()).isFalse();
            }
        }
    }

    private static ResumableChangeStreamEvent<BsonDocument> event(int sequence, boolean document) {
        final var token = new BsonDocument("_data", new BsonString("token-" + sequence));
        if (!document) {
            return new ResumableChangeStreamEvent<>(token);
        }
        final var raw = BsonDocument.parse("{ns:{db:'test',coll:'names'},documentKey:{_id:1}}")
                .append("_id", token)
                .append("operationType", new BsonString("insert"))
                .append("clusterTime", new BsonTimestamp(100, sequence));
        final var change = ChangeStreamDocument.createCodec(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                .decode(new BsonDocumentReader(raw), DecoderContext.builder().build());
        return new ResumableChangeStreamEvent<>(change);
    }

    private static final class CursorFixture implements AutoCloseable {
        private final EventFetcher<BsonDocument> fetcher;
        private final BufferingChangeStreamCursor<BsonDocument> cursor;
        private final Method enqueue;
        private final ReentrantLock lock;
        private final Condition eventAvailable;
        private final Condition resumed;

        private CursorFixture(int capacity, Duration pollInterval) throws ReflectiveOperationException {
            // Exercise the real buffer and cursor without opening a server connection.
            // This fixture supplies already fetched events; run() and its collaborators
            // are deliberately not used.
            fetcher = new EventFetcher<>(null, capacity, null, Clock.SYSTEM, Duration.ofMillis(1));
            cursor = new BufferingChangeStreamCursor<>(fetcher, Executors.newSingleThreadExecutor(), pollInterval);
            enqueue = EventFetcher.class.getDeclaredMethod("enqueue", ResumableChangeStreamEvent.class);
            enqueue.setAccessible(true);
            final var lockField = EventFetcher.class.getDeclaredField("lock");
            lockField.setAccessible(true);
            lock = (ReentrantLock) lockField.get(fetcher);
            final var eventAvailableField = EventFetcher.class.getDeclaredField("eventAvailable");
            eventAvailableField.setAccessible(true);
            eventAvailable = (Condition) eventAvailableField.get(fetcher);
            final var resumedField = EventFetcher.class.getDeclaredField("resumed");
            resumedField.setAccessible(true);
            resumed = (Condition) resumedField.get(fetcher);
        }

        @SuppressWarnings("unchecked")
        private void fail(Throwable failure) throws ReflectiveOperationException {
            final var error = EventFetcher.class.getDeclaredField("error");
            error.setAccessible(true);
            ((AtomicReference<Throwable>) error.get(fetcher)).set(failure);
            // run() publishes the failure before close() in its finally block.
            fetcher.close();
        }

        @SuppressWarnings("unchecked")
        private void markFetcherRunning() throws ReflectiveOperationException {
            // Seed the lifecycle without opening a MongoDB connection. The tests
            // exercise the real pause, resume, close, and buffer operations.
            final var state = EventFetcher.class.getDeclaredField("state");
            state.setAccessible(true);
            assertThat(((AtomicReference<State>) state.get(fetcher)).compareAndSet(State.NEW, State.RUNNING)).isTrue();
        }

        private void awaitBlockedOnWaitLock(Thread thread) {
            await().atMost(TEST_TIMEOUT).until(() -> lock.hasQueuedThread(thread));
        }

        private void whenWaiting(Condition condition, ThrowingRunnable<Exception> action) {
            await().atMost(TEST_TIMEOUT)
                    .until(() -> {
                        lock.lock();
                        try {
                            if (!lock.hasWaiters(condition)) {
                                return false;
                            }
                            // Observe and act under the same lock so that the waiter
                            // cannot advance between the check and the notification.
                            action.run();
                            return true;
                        }
                        finally {
                            lock.unlock();
                        }
                    });
        }

        private boolean enqueue(ResumableChangeStreamEvent<BsonDocument> event) throws Exception {
            try {
                return (boolean) enqueue.invoke(fetcher, event);
            }
            catch (InvocationTargetException e) {
                if (e.getCause() instanceof Exception exception) {
                    throw exception;
                }
                throw e;
            }
        }

        @Override
        public void close() {
            cursor.close();
        }
    }

    private static final class AsyncOperation<T> implements AutoCloseable {
        private final FutureTask<T> result;
        private final Thread thread;

        private AsyncOperation(Callable<T> action) {
            result = new FutureTask<>(action);
            thread = new Thread(result, "mongodb-cursor-poll-test");
            thread.start();
        }

        private T get() throws Exception {
            return result.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() throws InterruptedException {
            thread.interrupt();
            thread.join(TEST_TIMEOUT.toMillis());
            assertThat(thread.isAlive()).isFalse();
        }
    }
}
