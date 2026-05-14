/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.chroniclequeue;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.doc.FixFor;
import io.debezium.pipeline.DataChangeEvent;

/**
 * Unit tests for the link {@link HybridChronicleQueueProvider}.
 *
 * @author Chris Cranford
 */
class HybridChronicleQueueProviderTest {

    @TempDir
    Path tempDir;

    @Test
    @FixFor("dbz#1938")
    void shouldReturnNullOnEmptyPoll() throws InterruptedException {
        HybridChronicleQueueProvider provider = createProvider(10);

        assertThat(provider.poll()).isNull();
        assertThat(provider.size()).isEqualTo(0);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldEnqueueAndPollBelowCapacity() throws InterruptedException {
        HybridChronicleQueueProvider provider = createProvider(10);

        provider.enqueue(createTestEvent("a"));
        provider.enqueue(createTestEvent("b"));
        provider.enqueue(createTestEvent("c"));

        assertThat(provider.size()).isEqualTo(3);

        assertThat(getValue(provider.poll())).isEqualTo("a");
        assertThat(getValue(provider.poll())).isEqualTo("b");
        assertThat(getValue(provider.poll())).isEqualTo("c");
        assertThat(provider.poll()).isNull();
        assertThat(provider.size()).isEqualTo(0);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldEnqueueAndPollAtExactCapacity() throws InterruptedException {
        int capacity = 5;
        HybridChronicleQueueProvider provider = createProvider(capacity);

        for (int i = 0; i < capacity; i++) {
            provider.enqueue(createTestEvent("event-" + i));
        }

        assertThat(provider.size()).isEqualTo(capacity);

        for (int i = 0; i < capacity; i++) {
            assertThat(getValue(provider.poll())).isEqualTo("event-" + i);
        }
        assertThat(provider.poll()).isNull();
        assertThat(provider.size()).isEqualTo(0);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldSpillToChronicleQueueOnOverflow() throws InterruptedException {
        int capacity = 5;
        int total = capacity + 3;
        HybridChronicleQueueProvider provider = createProvider(capacity);

        for (int i = 0; i < total; i++) {
            provider.enqueue(createTestEvent("event-" + i));
        }

        assertThat(provider.size()).isEqualTo(total);

        for (int i = 0; i < total; i++) {
            DataChangeEvent event = provider.poll();
            assertThat(event).isNotNull();
            assertThat(getValue(event)).isEqualTo("event-" + i);
        }
        assertThat(provider.poll()).isNull();
        assertThat(provider.size()).isEqualTo(0);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldTrackSizeAcrossDequeAndChronicleQueue() throws InterruptedException {
        int capacity = 3;
        HybridChronicleQueueProvider provider = createProvider(capacity);

        assertThat(provider.size()).isEqualTo(0);

        provider.enqueue(createTestEvent("a"));
        assertThat(provider.size()).isEqualTo(1);

        provider.enqueue(createTestEvent("b"));
        assertThat(provider.size()).isEqualTo(2);

        provider.enqueue(createTestEvent("c"));
        assertThat(provider.size()).isEqualTo(3);

        provider.enqueue(createTestEvent("d"));
        assertThat(provider.size()).isEqualTo(4);

        provider.enqueue(createTestEvent("e"));
        assertThat(provider.size()).isEqualTo(5);

        provider.poll();
        assertThat(provider.size()).isEqualTo(4);

        provider.poll();
        assertThat(provider.size()).isEqualTo(3);

        provider.poll();
        assertThat(provider.size()).isEqualTo(2);

        provider.poll();
        assertThat(provider.size()).isEqualTo(1);

        provider.poll();
        assertThat(provider.size()).isEqualTo(0);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldPreserveFifoOrderWithSustainedHighTraffic() throws InterruptedException {
        int capacity = 5;
        int total = capacity * 3;
        HybridChronicleQueueProvider provider = createProvider(capacity);

        for (int i = 0; i < total; i++) {
            provider.enqueue(createTestEvent("event-" + i));
        }

        assertThat(provider.size()).isEqualTo(total);

        for (int i = 0; i < total; i++) {
            DataChangeEvent event = provider.poll();
            assertThat(event).isNotNull();
            assertThat(getValue(event)).isEqualTo("event-" + i);
        }
        assertThat(provider.poll()).isNull();
        assertThat(provider.size()).isEqualTo(0);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldHandleMixedEnqueueAndPoll() throws InterruptedException {
        int capacity = 3;
        HybridChronicleQueueProvider provider = createProvider(capacity);

        provider.enqueue(createTestEvent("a"));
        provider.enqueue(createTestEvent("b"));
        assertThat(getValue(provider.poll())).isEqualTo("a");

        provider.enqueue(createTestEvent("c"));
        provider.enqueue(createTestEvent("d"));
        provider.enqueue(createTestEvent("e"));

        assertThat(provider.size()).isEqualTo(4);

        assertThat(getValue(provider.poll())).isEqualTo("b");
        assertThat(getValue(provider.poll())).isEqualTo("c");
        assertThat(getValue(provider.poll())).isEqualTo("d");
        assertThat(getValue(provider.poll())).isEqualTo("e");
        assertThat(provider.poll()).isNull();
        assertThat(provider.size()).isEqualTo(0);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldCleanUpTempDirectoryOnClose() {
        HybridChronicleQueueProvider provider = new HybridChronicleQueueProvider();
        provider.configure(Map.of("max.queue.size", "10"));

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldConfigureWithExplicitPath() {
        Path queueDir = tempDir.resolve("explicit-hybrid-queue");
        HybridChronicleQueueProvider provider = new HybridChronicleQueueProvider();
        provider.configure(Map.of(
                HybridChronicleQueueProvider.QUEUE_PATH_PROPERTY, queueDir.toString(),
                "max.queue.size", "10"));

        assertThat(provider.size()).isEqualTo(0);
        assertThat(Files.exists(queueDir)).isTrue();
        provider.close();

        assertThat(Files.exists(queueDir)).isTrue();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldHandleMultipleOverflowCycles() throws InterruptedException {
        int capacity = 3;
        HybridChronicleQueueProvider provider = createProvider(capacity);

        for (int cycle = 0; cycle < 3; cycle++) {
            int total = capacity + 2;
            for (int i = 0; i < total; i++) {
                provider.enqueue(createTestEvent("c" + cycle + "-e" + i));
            }
            assertThat(provider.size()).isEqualTo(total);

            for (int i = 0; i < total; i++) {
                DataChangeEvent event = provider.poll();
                assertThat(event).isNotNull();
                assertThat(getValue(event)).isEqualTo("c" + cycle + "-e" + i);
            }
            assertThat(provider.size()).isEqualTo(0);
        }

        provider.close();
    }

    private HybridChronicleQueueProvider createProvider(int inMemorySize) {
        Path queueDir = tempDir.resolve("test-hybrid-" + System.nanoTime());
        HybridChronicleQueueProvider provider = new HybridChronicleQueueProvider();
        provider.configure(Map.of(
                HybridChronicleQueueProvider.QUEUE_PATH_PROPERTY, queueDir.toString(),
                "max.queue.size", String.valueOf(inMemorySize)));
        return provider;
    }

    private DataChangeEvent createTestEvent(String data) {
        Schema valueSchema = SchemaBuilder.struct()
                .field("data", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema).put("data", data);

        SourceRecord record = new SourceRecord(
                Map.of("server", "db1"),
                Map.of("pos", 1),
                "test-topic", null,
                null, null,
                valueSchema, value,
                null,
                new ConnectHeaders());

        return new DataChangeEvent(record);
    }

    private String getValue(DataChangeEvent event) {
        return ((Struct) event.getRecord().value()).getString("data");
    }
}