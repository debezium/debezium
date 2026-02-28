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

    private static Thread getWriter(ChangeEventQueue<DataChangeEvent> queue, int noOfEvents) {
        return new Thread(() -> {
            for (int i = 0; i < noOfEvents; i++) {
                try {
                    queue.enqueue(EVENT);
                }
                catch (InterruptedException ex) {
                    // exit thread
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
                    // exit thread
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
