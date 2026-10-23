/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.events;

import java.io.Closeable;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.metrics.MongoDbStreamingChangeEventSourceMetrics;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.Threads;

/**
 * An implementation of {@link  MongoChangeStreamCursor} which immediately starts consuming available events into a buffer.
 * <p>
 * Internally this cursor starts a {@link EventFetcher} as a separate thread on provided executor.
 * Although the implementation is internally thread safe the cursors is not meant to be accessed concurrently from multiple threads.
 *
 * @param <TResult> the type of documents the cursor contains
 */
@NotThreadSafe
public class BufferingChangeStreamCursor<TResult> implements MongoChangeStreamCursor<BufferingChangeStreamCursor.ResumableChangeStreamEvent<TResult>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferingChangeStreamCursor.class);
    public static final int THROTTLE_NO_MESSAGE_BEFORE_PAUSE = 5;
    public static final int FETCHER_SHUTDOWN_TIMEOUT = 30;

    private final EventFetcher<TResult> fetcher;
    private final ExecutorService executor;
    private final long maxSleepMillis;
    private BsonDocument lastResumeToken = null;

    /**
     * Combination of change stream event and resume token
     *
     * @param <TResult> the type of change stream document
     */
    @Immutable
    public static final class ResumableChangeStreamEvent<TResult> {
        public final Optional<ChangeStreamDocument<TResult>> document;
        /**
         * When {@link #document} is present this field corresponds to {@link ChangeStreamDocument#getResumeToken()}
         * Otherwise the value corresponds to the value returned by associated {@link MongoChangeStreamCursor#getResumeToken()}
         */
        public final BsonDocument resumeToken;

        /**
         * Creates resumable event from document
         *
         * @param document change stream event
         */
        public ResumableChangeStreamEvent(ChangeStreamDocument<TResult> document) {
            Objects.requireNonNull(document);
            this.document = Optional.of(document);
            this.resumeToken = document.getResumeToken();
        }

        /**
         * Creates resumable event from resume token
         *
         * @param resumeToken resume token
         */
        public ResumableChangeStreamEvent(BsonDocument resumeToken) {
            Objects.requireNonNull(resumeToken);
            this.document = Optional.empty();
            this.resumeToken = resumeToken;
        }

        public boolean isEmpty() {
            return document.isEmpty();
        }

        public boolean hasDocument() {
            return document.isPresent();
        }

        @Override
        public String toString() {
            return document
                    .map(ChangeStreamDocument::toString)
                    .orElseGet(resumeToken::toString);
        }
    }

    /**
     * Runnable responsible for fetching events from {@link ChangeStreamIterable} and buffering them in provided queue;
     * <p>
     * This utilises standard cursors returned by {@link ChangeStreamIterable#cursor()}
     *
     * @param <TResult>
     */
    public static final class EventFetcher<TResult> implements Runnable, Closeable {

        enum State {
            NEW,
            RUNNING,
            CLOSED
        }

        public static final long QUEUE_OFFER_TIMEOUT_MS = 100;

        private final ChangeStreamIterable<TResult> stream;
        private final Semaphore capacity;
        private final Queue<ResumableChangeStreamEvent<TResult>> queue;
        private final DelayStrategy throttler;
        private final AtomicReference<State> state;
        private final AtomicReference<MongoChangeStreamCursor<ChangeStreamDocument<TResult>>> cursorRef;
        private final AtomicReference<Throwable> error;
        private final MongoDbStreamingChangeEventSourceMetrics metrics;
        private final Clock clock;
        private int noMessageIterations = 0;
        private final Lock lock = new ReentrantLock();
        private final Condition resumed = lock.newCondition();
        private final Condition eventAvailable = lock.newCondition();
        private volatile boolean paused;
        private volatile boolean consumerWaiting;

        public EventFetcher(ChangeStreamIterable<TResult> stream,
                            int capacity,
                            MongoDbStreamingChangeEventSourceMetrics metrics,
                            Clock clock,
                            DelayStrategy throttler) {
            this.stream = stream;
            this.capacity = new Semaphore(capacity);
            this.metrics = metrics;
            this.clock = clock;
            this.throttler = throttler;
            this.state = new AtomicReference<>(State.NEW);
            this.cursorRef = new AtomicReference<>(null);
            this.queue = new ConcurrentLinkedQueue<>();
            this.error = new AtomicReference<>(null);
        }

        public EventFetcher(ChangeStreamIterable<TResult> stream,
                            int capacity,
                            MongoDbStreamingChangeEventSourceMetrics metrics,
                            Clock clock,
                            Duration throttleMaxSleep) {
            this(stream, capacity, metrics, clock, DelayStrategy.constant(throttleMaxSleep));
        }

        /**
         * Indicates whether the internal cursor has been opened and the fetcher has not been closed.
         * Pausing event fetching does not change this lifecycle state.
         *
         * @return true if running, false otherwise
         */
        public boolean isRunning() {
            return state.get() == State.RUNNING;
        }

        private boolean isClosed() {
            return state.get() == State.CLOSED;
        }

        /**
         * Indicates whether an error occurred during event fetching
         *
         * @return true if error occurred, false otherwise
         */
        public boolean hasError() {
            return error.get() != null;
        }

        /**
         * Returns error that occurred during event fetching
         *
         * @return error or null if no error occurred
         */
        public Throwable getError() {
            return error.get();
        }

        @Override
        public void close() {
            state.set(State.CLOSED);
            lock.lock();
            try {
                eventAvailable.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        public boolean isPaused() {
            return paused;
        }

        public void pause() {
            paused = true;
            LOGGER.trace("Event buffering will now pause.");
        }

        public void resume() {
            lock.lock();
            try {
                paused = false;
                resumed.signalAll();
                LOGGER.trace("Event buffering will now resume.");
            }
            finally {
                lock.unlock();
            }
        }

        public void waitIfPaused() throws InterruptedException {
            lock.lock();
            try {
                while (paused) {
                    LOGGER.trace("Waiting until buffering is resumed.");
                    resumed.await();
                }
            }
            finally {
                lock.unlock();
            }
        }

        public ResumableChangeStreamEvent<TResult> poll() {
            var event = queue.poll();
            if (event == null) {
                if (hasError()) {
                    throw new DebeziumException("Unable to fetch change stream events", getError());
                }
            }
            else {
                capacity.release();
            }
            return event;
        }

        void awaitEvent(long timeoutMillis) throws InterruptedException {
            lock.lockInterruptibly();
            try {
                // Register before checking the queue so that an enqueue cannot be missed
                // between the empty check and releasing the lock in awaitNanos().
                consumerWaiting = true;
                long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
                while (queue.isEmpty() && !hasError() && !isClosed() && remainingNanos > 0) {
                    remainingNanos = eventAvailable.awaitNanos(remainingNanos);
                }
            }
            finally {
                consumerWaiting = false;
                lock.unlock();
            }
        }

        private void signalEventAvailable() {
            // Avoid taking a lock for every event while the consumer is draining a backlog.
            if (consumerWaiting) {
                lock.lock();
                try {
                    eventAvailable.signal();
                }
                finally {
                    lock.unlock();
                }
            }
        }

        public boolean isEmpty() {
            return queue.isEmpty();
        }

        /**
         * Depending on queue implementation this method may not be reliable
         * By default see {@link ConcurrentLinkedQueue#size()}
         *
         * @return approximate number of elements in queue
         */
        public int size() {
            return queue.size();
        }

        @Override
        public void run() {
            if (isClosed()) {
                return;
            }
            try (MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor = stream.cursor()) {
                cursorRef.compareAndSet(null, cursor);
                // close() may have been called while opening the MongoDB cursor.
                if (!state.compareAndSet(State.NEW, State.RUNNING)) {
                    return;
                }
                noMessageIterations = 0;
                fetchEvents(cursor);
            }
            catch (InterruptedException e) {
                if (!isClosed()) {
                    error.set(e);
                    LOGGER.error("Fetcher thread interrupted", e);
                }
                Thread.currentThread().interrupt();
            }
            catch (Throwable e) {
                error.set(e);
                LOGGER.error("Fetcher thread has failed", e);
            }
            finally {
                cursorRef.set(null);
                close();
            }
        }

        private void fetchEvents(MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor) throws InterruptedException {
            ResumableChangeStreamEvent<TResult> lastEvent = null;
            var repeat = false;
            while (isRunning()) {
                if (!repeat) {
                    if (paused) {
                        waitIfPaused();
                    }
                    var maybeEvent = fetchEvent(cursor);
                    if (maybeEvent.isEmpty()) {
                        LOGGER.warn("Resume token not available on this poll");
                        continue;
                    }
                    lastEvent = maybeEvent.get();
                }
                repeat = !enqueue(lastEvent);
            }
        }

        private Optional<ResumableChangeStreamEvent<TResult>> fetchEvent(MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor) {
            var beforeEventPollTime = clock.currentTimeAsInstant();
            var document = cursor.tryNext();
            metrics.onSourceEventPolled(document, clock, beforeEventPollTime);
            throttleIfNeeded(document);

            // Only create resumable event if we have either document or cursor resume token
            // Cursor resume token may be `null` in case of issues like SERVER-63772, and situations called out in the Javadocs:
            // > resume token [...] can be null if the cursor has either not been iterated yet, or the cursor is closed.
            return Optional.<ResumableChangeStreamEvent<TResult>> empty()
                    .or(() -> Optional.ofNullable(document).map(ResumableChangeStreamEvent::new))
                    .or(() -> Optional.ofNullable(cursor.getResumeToken()).map(ResumableChangeStreamEvent::new));
        }

        private void throttleIfNeeded(ChangeStreamDocument<TResult> document) {
            if (document == null) {
                noMessageIterations++;
            }
            if (noMessageIterations >= THROTTLE_NO_MESSAGE_BEFORE_PAUSE) {
                LOGGER.debug("Sleeping after {} empty polls", noMessageIterations);
                throttler.sleepWhen(true);
                noMessageIterations = 0;
            }
        }

        private boolean enqueue(ResumableChangeStreamEvent<TResult> event) throws InterruptedException {
            var available = this.capacity.tryAcquire(QUEUE_OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (!available) {
                LOGGER.warn("Unable to acquire buffer lock, buffer queue is likely full");
                return false;
            }
            // ConcurrentLinkedQueue.offer() always succeeds.
            queue.offer(event);
            signalEventAvailable();
            return true;
        }
    }

    public static <TResult> BufferingChangeStreamCursor<TResult> fromIterable(
                                                                              ChangeStreamIterable<TResult> stream,
                                                                              MongoDbTaskContext taskContext,
                                                                              MongoDbStreamingChangeEventSourceMetrics metrics,
                                                                              Clock clock) {
        var config = taskContext.getConfig();

        return new BufferingChangeStreamCursor<>(
                new EventFetcher<>(stream, config.getMaxBatchSize(), metrics, clock, config.getPollInterval()),
                Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.getServerName(), "replicator-fetcher", 1),
                config.getPollInterval());
    }

    /**
     * Creates new prefetching cursor
     *
     * @param fetcher MongoDB change event fetcher
     * @param executor executor used to dispatch buffering thread
     * @param throttleMaxSleep maximum duration of an individual wait for a buffered event
     */
    public BufferingChangeStreamCursor(EventFetcher<TResult> fetcher, ExecutorService executor, Duration throttleMaxSleep) {
        this.fetcher = fetcher;
        this.executor = executor;
        this.maxSleepMillis = throttleMaxSleep.toMillis();
        if (maxSleepMillis <= 1) {
            throw new IllegalArgumentException("Maximum delay must be greater than initial delay");
        }
    }

    public BufferingChangeStreamCursor<TResult> start() {
        LOGGER.info("Fetcher submitted for execution: {} @ {}", fetcher, executor);
        executor.submit(fetcher);
        return this;
    }

    @Override
    public ResumableChangeStreamEvent<TResult> tryNext() {
        try {
            return tryNextInterruptibly();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DebeziumException("Interrupted while waiting for change stream events", e);
        }
    }

    /**
     * Reads the next buffered event, allowing the streaming loop to handle shutdown
     * interruption separately from a failure to fetch events.
     */
    public ResumableChangeStreamEvent<TResult> tryNextInterruptibly() throws InterruptedException {
        var event = pollWithDelay();
        if (event != null) {
            lastResumeToken = event.resumeToken;
        }
        return event;
    }

    /**
     * Returns next event in buffer.
     * Not that unlike other Mongo implementation this method does not block
     *
     * @throws NoSuchElementException when no element is available
     * @return event
     */
    @Override
    public ResumableChangeStreamEvent<TResult> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return tryNext();
    }

    /**
     * Repeatedly polling fetcher for new event, exponentially waiting between polls until limit is reached
     *
     * @return event or null if not available within time limit
     */
    private ResumableChangeStreamEvent<TResult> pollWithDelay() throws InterruptedException {
        long delayMillis = 1;
        while (true) {
            final var event = fetcher.poll();
            if (event != null || fetcher.isClosed()) {
                return event;
            }

            fetcher.awaitEvent(delayMillis);

            // Keep the existing bounded exponential timeout (1, 2, ... max), but
            // let an arriving event end any individual wait immediately.
            if (delayMillis == maxSleepMillis) {
                return fetcher.poll();
            }
            delayMillis += Math.min(delayMillis, maxSleepMillis - delayMillis);
        }
    }

    public void resume() {
        fetcher.resume();
    }

    public void pause() {
        fetcher.pause();
    }

    public boolean isPaused() {
        return fetcher.isPaused();
    }

    @Override
    public boolean hasNext() {
        return !fetcher.isEmpty();
    }

    /**
     * See {@link EventFetcher#size()}
     */
    @Override
    public int available() {
        return fetcher.size();
    }

    @Override
    public BsonDocument getResumeToken() {
        return lastResumeToken;
    }

    @Override
    public ServerCursor getServerCursor() {
        return fetcher.cursorRef.get().getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return fetcher.cursorRef.get().getServerAddress();
    }

    /**
     * Returns the address of the server hosting the underlying cursor, if the fetcher has opened it.
     */
    public Optional<ServerAddress> getCurrentServerAddress() {
        final var cursor = fetcher.cursorRef.get();
        if (cursor == null) {
            return Optional.empty();
        }

        try {
            return Optional.ofNullable(cursor.getServerAddress());
        }
        catch (IllegalStateException e) {
            // The fetcher closed the cursor concurrently; the address is simply unavailable.
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        fetcher.close();
        executor.shutdown();
        try {
            LOGGER.info("Awaiting fetcher thread termination");
            executor.awaitTermination(FETCHER_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while waiting for fetcher thread shutdown");
        }
    }
}
