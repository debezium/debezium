/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.LoggingContext;

// Warning: These tests require average processing speed to pass.
// Therefore, if the processing speed is below the buffer included in the tests, they may fail.
public class ChangeEventQueueTest {

    private static final DataChangeEvent EVENT = getDataChangeEvent();

    static Stream<Arguments> data() {
        int[] writers = { 1, 2, 4, 8, 16 };
        int[] readers = { 1, 2, 4, 8, 16 };
        int totalEvents = 1_000_000;

        return java.util.Arrays.stream(writers).boxed()
                .flatMap(writer -> java.util.Arrays.stream(readers).boxed()
                        .map(reader -> Arguments.of(writer, reader, totalEvents)));
    }

    static Stream<Arguments> pollWithDispatchData() {
        int[] writers = { 1, 2, 4, 8, 16 };
        int[] readers = { 1, 2, 4, 8, 16 };
        int totalEvents = 1_000;

        return java.util.Arrays.stream(writers).boxed()
                .flatMap(writer -> java.util.Arrays.stream(readers).boxed()
                        .map(reader -> Arguments.of(writer, reader, totalEvents)));
    }

    @ParameterizedTest(name = "{index}: testQueue({0} writers, {1} readers, {2} events)")
    @MethodSource("data")
    void shouldQueueAndPollMessages(int noOfWriters, int noOfReaders, int noOfEventsPerWriter) throws InterruptedException {
        long totalNoOfEvents = (long) noOfWriters * noOfEventsPerWriter;
        Thread[] writers = new Thread[noOfWriters];
        Thread[] readers = new Thread[noOfReaders];
        AtomicLong recordsRead = new AtomicLong();

        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(8192)
                .maxQueueSize(8192 * 2)
                .queueProvider(new DefaultQueueProvider<>(8192 * 2))
                .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
                .pollInterval(Duration.ofMillis(500))
                .pollDispatchInterval(Duration.ofMillis(0))
                .build();

        for (int i = 0; i < noOfWriters; i++) {
            writers[i] = getWriter(queue, noOfEventsPerWriter);
        }
        for (int i = 0; i < noOfReaders; i++) {
            readers[i] = getReader(queue, totalNoOfEvents, recordsRead);
        }

        try {
            for (Thread thread : writers) {
                thread.start();
            }
            for (Thread thread : readers) {
                thread.start();
            }

            // 1_000_000 Events should write / read in about 1-2 seconds
            // At max, we can wait up to 10 seconds
            long maxWaitTimeout = TimeUnit.SECONDS.toMillis(10);

            for (Thread writer : writers) {
                writer.join(maxWaitTimeout);
            }
            for (Thread reader : readers) {
                reader.join(maxWaitTimeout);
            }
            assertEquals(totalNoOfEvents, recordsRead.get());
        }
        finally {
            for (Thread thread : writers) {
                thread.interrupt();
            }
            for (Thread thread : readers) {
                thread.interrupt();
            }
        }
    }

    @ParameterizedTest(name = "{index}: testQueue({0} writers, {1} readers, {2} events)")
    @MethodSource("pollWithDispatchData")
    void shouldPollMessagesAfterDispatchTimeout(int noOfWriters, int noOfReaders, int noOfEventsPerWriter) throws InterruptedException {
        long totalNoOfEvents = (long) noOfWriters * noOfEventsPerWriter;
        Thread[] writers = new Thread[noOfWriters];
        Thread[] readers = new Thread[noOfReaders];
        AtomicLong recordsRead = new AtomicLong();

        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(8192 * 2)
                .maxQueueSize(8192 * 2)
                .queueProvider(new DefaultQueueProvider<>(8192 * 2))
                .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
                .pollInterval(Duration.ofMillis(500))
                .pollDispatchInterval(Duration.ofMillis(1500))
                .build();

        for (int i = 0; i < noOfWriters; i++) {
            writers[i] = getWriter(queue, noOfEventsPerWriter);
        }
        for (int i = 0; i < noOfReaders; i++) {
            readers[i] = getReader(queue, totalNoOfEvents, recordsRead);
        }

        try {
            for (Thread thread : writers) {
                thread.start();
            }
            for (Thread thread : readers) {
                thread.start();
            }

            // 1_000 Events should be written/read in about 2 seconds ( dispatchInterval + pollInterval -> waiting for Queue to be full to complete batchSize)
            // After 3 seconds there should be no records read
            long maxWaitTimeout = TimeUnit.SECONDS.toMillis(3);
            long deadline = System.currentTimeMillis() + maxWaitTimeout;

            for (Thread writer : writers) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining > 0) {
                    writer.join(remaining);
                }
            }
            for (Thread reader : readers) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining > 0) {
                    reader.join(remaining);
                }
            }
            assertEquals(totalNoOfEvents, recordsRead.get());
        }
        finally {
            for (Thread thread : writers) {
                thread.interrupt();
            }
            for (Thread thread : readers) {
                thread.interrupt();
            }
        }
    }

    @ParameterizedTest(name = "{index}: testQueue({0} writers, {1} readers, {2} events)")
    @MethodSource("pollWithDispatchData")
    void shouldNotPollMessagesBeforeDispatchTimeout(int noOfWriters, int noOfReaders, int noOfEventsPerWriter) throws InterruptedException {
        long totalNoOfEvents = (long) noOfWriters * noOfEventsPerWriter;
        Thread[] writers = new Thread[noOfWriters];
        Thread[] readers = new Thread[noOfReaders];
        AtomicLong recordsRead = new AtomicLong();

        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(8192)
                .maxQueueSize(8192 * 2)
                .queueProvider(new DefaultQueueProvider<>(8192 * 2))
                .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
                .pollInterval(Duration.ofMillis(500))
                .pollDispatchInterval(Duration.ofMillis(4000))
                .build();

        for (int i = 0; i < noOfWriters; i++) {
            writers[i] = getWriter(queue, noOfEventsPerWriter);
        }
        for (int i = 0; i < noOfReaders; i++) {
            readers[i] = getReader(queue, totalNoOfEvents, recordsRead);
        }

        try {
            for (Thread thread : writers) {
                thread.start();
            }
            for (Thread thread : readers) {
                thread.start();
            }

            // No Event Should be read before 2 seconds
            long maxWaitTimeout = TimeUnit.SECONDS.toMillis(2);
            long deadline = System.currentTimeMillis() + maxWaitTimeout;

            for (Thread writer : writers) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining > 0) {
                    writer.join(remaining);
                }
            }
            for (Thread reader : readers) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining > 0) {
                    reader.join(remaining);
                }
            }
            assertEquals(0, recordsRead.get());
        }
        finally {
            for (Thread thread : writers) {
                thread.interrupt();
            }
            for (Thread thread : readers) {
                thread.interrupt();
            }
        }
    }

    @ParameterizedTest(name = "{index}: testQueue({0} writers, {1} readers, {2} events)")
    @MethodSource("pollWithDispatchData")
    void shouldPollMessagesBeforeDispatchTimeoutIfQueueFull(int noOfWriters, int noOfReaders, int noOfEventsPerWriter) throws InterruptedException {
        long totalNoOfEvents = (long) noOfWriters * noOfEventsPerWriter;
        Thread[] writers = new Thread[noOfWriters];
        Thread[] readers = new Thread[noOfReaders];
        AtomicLong recordsRead = new AtomicLong();

        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(1000)
                .maxQueueSize(1000)
                .queueProvider(new DefaultQueueProvider<>(1000))
                .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
                .pollInterval(Duration.ofMillis(500))
                .pollDispatchInterval(Duration.ofMillis(4000))
                .build();

        for (int i = 0; i < noOfWriters; i++) {
            writers[i] = getWriter(queue, noOfEventsPerWriter);
        }
        for (int i = 0; i < noOfReaders; i++) {
            readers[i] = getReader(queue, totalNoOfEvents, recordsRead);
        }

        try {
            for (Thread thread : writers) {
                thread.start();
            }
            for (Thread thread : readers) {
                thread.start();
            }

            // 1_000 Events should read/write in about 1-2 seconds. Ignoring dispatchInterval when Queue is Full.
            // After 2 seconds all records should be read
            long maxWaitTimeout = TimeUnit.SECONDS.toMillis(2);
            long deadline = System.currentTimeMillis() + maxWaitTimeout;

            for (Thread writer : writers) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining > 0) {
                    writer.join(remaining);
                }
            }
            for (Thread reader : readers) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining > 0) {
                    reader.join(remaining);
                }
            }
            assertEquals(totalNoOfEvents, recordsRead.get());
        }
        finally {
            for (Thread thread : writers) {
                thread.interrupt();
            }
            for (Thread thread : readers) {
                thread.interrupt();
            }
        }
    }

    private static Thread getWriter(ChangeEventQueue<DataChangeEvent> queue, int noOfEvents) {
        return new Thread(() -> {
            for (int i = 0; i < noOfEvents; i++) {
                try {
                    queue.enqueue(EVENT);
                }
                catch (InterruptedException ex) {
                    break;
                }
            }
        });
    }

    private static Thread getReader(ChangeEventQueue<DataChangeEvent> queue, long totalNoOfEvents, AtomicLong recordsRead) {
        return new Thread(() -> {
            while (recordsRead.get() < totalNoOfEvents) {
                try {
                    recordsRead.addAndGet(queue.poll().size());
                }
                catch (InterruptedException ex) {
                    break;
                }
            }
        });
    }

    private static DataChangeEvent getDataChangeEvent() {
        Schema valueSchema = SchemaBuilder.struct().field("cdc", Schema.STRING_SCHEMA).build();
        return new DataChangeEvent(new SourceRecord(java.util.Collections.emptyMap(), java.util.Collections.emptyMap(), "dummy",
                valueSchema, new Struct(valueSchema).put("cdc", "Change Data Capture Even via Debezium")));
    }

}
