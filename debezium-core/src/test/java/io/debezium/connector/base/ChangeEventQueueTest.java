/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.LoggingContext;

@RunWith(Parameterized.class)
public class ChangeEventQueueTest {

    private static final DataChangeEvent EVENT = getDataChangeEvent();

    private final int noOfWriters;
    private final int noOfReaders;
    private final int noOfEventsPerWriter;
    private final long totalNoOfEvents;
    private final Thread[] writers;
    private final Thread[] readers;
    private final AtomicLong recordsRead;

    public ChangeEventQueueTest(int noOfWriters, int noOfReaders, int noOfEventsPerWriter) {
        this.noOfWriters = noOfWriters;
        this.noOfReaders = noOfReaders;
        this.noOfEventsPerWriter = noOfEventsPerWriter;
        this.totalNoOfEvents = (long) noOfWriters * noOfEventsPerWriter;
        this.writers = new Thread[noOfWriters];
        this.readers = new Thread[noOfReaders];
        this.recordsRead = new AtomicLong();
    }

    @Parameters(name = "{index}: testQueue({0} writers, {1} readers, {2} events)")
    public static Collection<Object[]> data() {
        int[] writers = { 1, 2, 4, 8, 16 };
        int[] readers = { 1, 2, 4, 8, 16 };
        int totalEvents = 1_000_000;
        Object[][] params = new Object[writers.length * readers.length][];
        int index = 0;
        for (int writer : writers) {
            for (int reader : readers) {
                params[index++] = new Object[]{ writer, reader, totalEvents };
            }
        }
        return Arrays.asList(params);
    }

    @Before
    public void setup() {
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(8192)
                .maxQueueSize(8192 * 2)
                .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
                .pollInterval(Duration.ofMillis(500))
                .build();
        for (int i = 0; i < noOfWriters; i++) {
            writers[i] = getWriter(queue, noOfEventsPerWriter);
        }
        for (int i = 0; i < noOfReaders; i++) {
            readers[i] = getReader(queue, totalNoOfEvents, recordsRead);
        }
    }

    @Test
    public void shouldQueueAndPollMessages() throws InterruptedException {
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

    @After
    public void teardown() {
        for (Thread thread : writers) {
            thread.interrupt();
        }
        for (Thread thread : readers) {
            thread.interrupt();
        }
    }

    private static Thread getWriter(ChangeEventQueue<DataChangeEvent> queue, int noOfEvents) {
        return new Thread(() -> {
            for (int i = 0; i < noOfEvents; i++) {
                try {
                    queue.doEnqueue(EVENT);
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
        return new DataChangeEvent(new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "dummy",
                valueSchema, new Struct(valueSchema).put("cdc", "Change Data Capture Even via Debezium")));
    }

}
