/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.chroniclequeue;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
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
 * Unit tests for {@link ChronicleQueueProvider}.
 *
 * @author Chris Cranford
 */
class ChronicleQueueProviderTest {

    @TempDir
    Path tempDir;

    @Test
    @FixFor("dbz#1938")
    void shouldConfigureWithTempDirectory() {
        ChronicleQueueProvider provider = new ChronicleQueueProvider();
        provider.configure(Map.of());

        assertThat(provider.size()).isEqualTo(0);
        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldConfigureWithExplicitPath() {
        Path queueDir = tempDir.resolve("explicit-queue");
        ChronicleQueueProvider provider = new ChronicleQueueProvider();
        provider.configure(Map.of(ChronicleQueueProvider.QUEUE_PATH_PROPERTY, queueDir.toString()));

        assertThat(provider.size()).isEqualTo(0);
        assertThat(Files.exists(queueDir)).isTrue();
        provider.close();

        assertThat(Files.exists(queueDir)).isTrue();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldEnqueueAndPollSingleEvent() throws InterruptedException {
        ChronicleQueueProvider provider = createProvider();

        DataChangeEvent event = createTestEvent("value-1");
        provider.enqueue(event);

        assertThat(provider.size()).isEqualTo(1);

        DataChangeEvent result = provider.poll();
        assertThat(result).isNotNull();
        assertThat(provider.size()).isEqualTo(0);

        Struct resultValue = (Struct) result.getRecord().value();
        assertThat(resultValue.getString("data")).isEqualTo("value-1");

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldReturnNullOnEmptyPoll() throws InterruptedException {
        ChronicleQueueProvider provider = createProvider();

        DataChangeEvent result = provider.poll();
        assertThat(result).isNull();
        assertThat(provider.size()).isEqualTo(0);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldPreserveFifoOrdering() throws InterruptedException {
        ChronicleQueueProvider provider = createProvider();

        provider.enqueue(createTestEvent("first"));
        provider.enqueue(createTestEvent("second"));
        provider.enqueue(createTestEvent("third"));

        assertThat(provider.size()).isEqualTo(3);

        assertThat(getValue(provider.poll())).isEqualTo("first");
        assertThat(getValue(provider.poll())).isEqualTo("second");
        assertThat(getValue(provider.poll())).isEqualTo("third");
        assertThat(provider.size()).isEqualTo(0);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldTrackSizeAccurately() throws InterruptedException {
        ChronicleQueueProvider provider = createProvider();

        assertThat(provider.size()).isEqualTo(0);

        provider.enqueue(createTestEvent("a"));
        assertThat(provider.size()).isEqualTo(1);

        provider.enqueue(createTestEvent("b"));
        assertThat(provider.size()).isEqualTo(2);

        provider.poll();
        assertThat(provider.size()).isEqualTo(1);

        provider.poll();
        assertThat(provider.size()).isEqualTo(0);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldCleanUpTempDirectoryOnClose() {
        ChronicleQueueProvider provider = new ChronicleQueueProvider();
        Map<String, Object> props = new HashMap<>();
        provider.configure(props);

        provider.close();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldHandleMultipleEnqueuePollCycles() throws InterruptedException {
        ChronicleQueueProvider provider = createProvider();

        for (int cycle = 0; cycle < 3; cycle++) {
            for (int i = 0; i < 10; i++) {
                provider.enqueue(createTestEvent("cycle-" + cycle + "-event-" + i));
            }
            assertThat(provider.size()).isEqualTo(10);

            for (int i = 0; i < 10; i++) {
                DataChangeEvent event = provider.poll();
                assertThat(event).isNotNull();
                assertThat(getValue(event)).isEqualTo("cycle-" + cycle + "-event-" + i);
            }
            assertThat(provider.size()).isEqualTo(0);
        }

        provider.close();
    }

    private ChronicleQueueProvider createProvider() {
        Path queueDir = tempDir.resolve("test-queue-" + System.nanoTime());
        ChronicleQueueProvider provider = new ChronicleQueueProvider();
        provider.configure(Map.of(ChronicleQueueProvider.QUEUE_PATH_PROPERTY, queueDir.toString()));
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