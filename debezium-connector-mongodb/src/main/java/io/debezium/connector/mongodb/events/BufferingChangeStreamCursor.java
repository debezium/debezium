/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.events;

import java.io.Closeable;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

    private final EventFetcher<TResult> fetcher;
    private final ExecutorService executor;
    private final DelayStrategy throttler;

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
         * Creates resumable event
         *
         * @param document change stream event
         * @param cursor cursors that produced the event
         */
        public ResumableChangeStreamEvent(ChangeStreamDocument<TResult> document, MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor) {
            this.document = Optional.ofNullable(document);
            this.resumeToken = this.document
                    .map(ChangeStreamDocument::getResumeToken)
                    .orElseGet(cursor::getResumeToken);
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

        private final ChangeStreamIterable<TResult> stream;
        private final Semaphore capacity;
        private final Queue<ResumableChangeStreamEvent<TResult>> queue;
        private final DelayStrategy throttler;
        private final AtomicBoolean running;
        private final AtomicReference<MongoChangeStreamCursor<ChangeStreamDocument<TResult>>> cursorRef;
        private final MongoDbStreamingChangeEventSourceMetrics metrics;
        private final Clock clock;
        private int noMessageIterations = 0;

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
            this.running = new AtomicBoolean(false);
            this.cursorRef = new AtomicReference<>(null);
            this.queue = new ConcurrentLinkedQueue<>();
        }

        public EventFetcher(ChangeStreamIterable<TResult> stream,
                            int capacity,
                            MongoDbStreamingChangeEventSourceMetrics metrics,
                            Clock clock,
                            Duration throttleMaxSleep) {
            this(stream, capacity, metrics, clock, DelayStrategy.constant(throttleMaxSleep));
        }

        /**
         * Indicates whether event fetching is running and the internal cursor is open
         *
         * @return true if running, false otherwise
         */
        public boolean isRunning() {
            return running.get();
        }

        @Override
        public void close() {
            running.set(false);
        }

        public ResumableChangeStreamEvent<TResult> poll() {
            var event = queue.poll();
            if (event != null) {
                capacity.release();
            }
            return event;
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
            try (MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor = stream.cursor()) {
                cursorRef.compareAndSet(null, cursor);
                running.set(true);
                noMessageIterations = 0;
                fetchEvents(cursor);
            }
            catch (InterruptedException e) {
                throw new DebeziumException("Fetcher thread interrupted", e);
            }
            finally {
                cursorRef.set(null);
                close();
            }
        }

        private void fetchEvents(MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor) throws InterruptedException {
            while (isRunning()) {
                var beforeEventPollTime = clock.currentTimeAsInstant();
                var event = new ResumableChangeStreamEvent<>(cursor.tryNext(), cursor);
                event.document.ifPresent(doc -> LOGGER.trace("Polled Change Stream event: {}", event));
                metrics.onSourceEventPolled(event.document.orElse(null), clock, beforeEventPollTime);

                if (event.hasDocument()) {
                    enqueue(event);
                }
                else {
                    throttleIfNeeded(event);
                }
            }
        }

        private void throttleIfNeeded(ResumableChangeStreamEvent<TResult> event) throws InterruptedException {
            noMessageIterations++;
            if (noMessageIterations >= THROTTLE_NO_MESSAGE_BEFORE_PAUSE) {
                enqueue(event);
                noMessageIterations = 0;
                throttler.sleepWhen(true);
            }
        }

        private void enqueue(ResumableChangeStreamEvent<TResult> event) throws InterruptedException {
            this.capacity.acquire();
            queue.offer(event);
        }
    }

    public static <TResult> BufferingChangeStreamCursor<TResult> fromIterable(
                                                                              ChangeStreamIterable<TResult> stream,
                                                                              MongoDbTaskContext taskContext,
                                                                              MongoDbStreamingChangeEventSourceMetrics metrics,
                                                                              Clock clock) {
        var config = taskContext.getConnectorConfig();

        return new BufferingChangeStreamCursor<>(
                new EventFetcher<>(stream, config.getMaxBatchSize(), metrics, clock, config.getPollInterval()),
                Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(), "replicator-buffer", 1),
                config.getPollInterval());
    }

    /**
     * Creates new prefetching cursor
     *
     * @param fetcher MongoDB change event fetcher
     * @param executor executor used to dispatch buffering thread
     * @param throttler throttling mechanism
     */
    public BufferingChangeStreamCursor(EventFetcher<TResult> fetcher, ExecutorService executor, DelayStrategy throttler) {
        this.fetcher = fetcher;
        this.executor = executor;
        this.throttler = throttler;
    }

    public BufferingChangeStreamCursor(EventFetcher<TResult> fetcher, ExecutorService executor, Duration throttleMaxSleep) {
        this(fetcher, executor, DelayStrategy.boundedExponential(Duration.ofMillis(1), throttleMaxSleep, 2));
    }

    public BufferingChangeStreamCursor<TResult> start() {
        executor.submit(fetcher);
        return this;
    }

    @Override
    public ResumableChangeStreamEvent<TResult> tryNext() {
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
    private ResumableChangeStreamEvent<TResult> pollWithDelay() {
        boolean slept;
        ResumableChangeStreamEvent<TResult> event;

        do {
            event = fetcher.poll();
            slept = throttler.sleepWhen(event == null);
        } while (slept);

        return event;
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

    @Override
    public void close() {
        fetcher.close();
        executor.shutdown();
    }
}
