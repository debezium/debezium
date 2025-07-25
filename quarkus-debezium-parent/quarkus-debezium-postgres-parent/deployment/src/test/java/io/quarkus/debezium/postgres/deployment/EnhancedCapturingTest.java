/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.postgres.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.engine.RecordChangeEvent;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.debezium.engine.deserializer.ObjectMapperDeserializer;
import io.quarkus.test.QuarkusUnitTest;
import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(value = DatabaseTestResource.class)
public class EnhancedCapturingTest {
    @Inject
    CaptureProductsHandler captureProductsHandler;

    @Inject
    Debezium debezium;

    @BeforeEach
    void setUp() {
        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(debezium.status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));
    }

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar
                    .addClasses(CaptureProductsHandler.class))
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .overrideConfigKey("quarkus.debezium.name", "test")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "dbserver1")
            .overrideConfigKey("quarkus.debezium.plugin.name", "pgoutput")
            .overrideConfigKey("quarkus.debezium.snapshot.mode", "initial")
            .overrideConfigKey("quarkus.debezium.capturing.orders.destination", "dbserver1.public.orders")
            .overrideConfigKey("quarkus.debezium.capturing.orders.deserializer", "io.quarkus.debezium.postgres.deployment.EnhancedCapturingTest.OrderDeserializer")
            .overrideConfigKey("quarkus.datasource.devservices.enabled", "false");

    @Test
    @DisplayName("should prefer the capture method with CapturingEvent<?> than RecordChangeEvent<?>")
    void shouldInvokeCaptureWithCapturingEvent() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureProductsHandler.isCapturingEventInvoked()).isTrue());

        assertThat(captureProductsHandler.isRecordChangeInvoked()).isFalse();
    }

    @Test
    @DisplayName("should call the filtered by destination capture")
    void shouldInvokeFilteredByDestinationCapture() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureProductsHandler.filteredEvent()).isEqualTo(2));

        assertThat(captureProductsHandler.isRecordChangeInvoked()).isFalse();
    }

    @DisplayName("should map and capture orders filtered by destination")
    void shouldMapAndCaptureOrdersFilteredByDestination() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureProductsHandler.getOrders()).containsExactlyInAnyOrder(
                        new Order(1, "one"),
                        new Order(1, "two")));
    }

    @ApplicationScoped
    static class CaptureProductsHandler {
        private final AtomicBoolean isRecordChangeInvoked = new AtomicBoolean(false);
        private final AtomicBoolean isCapturingEventInvoked = new AtomicBoolean(false);
        private final AtomicInteger isCapturingFilteredEvent = new AtomicInteger(0);
        private final List<Order> orders = new ArrayList<>();

        @Capturing()
        public void capture(RecordChangeEvent<SourceRecord> event) {
            isRecordChangeInvoked.set(true);
        }

        @Capturing()
        public void newCapture(CapturingEvent<SourceRecord> event) {
            isCapturingEventInvoked.set(true);
        }

        @Capturing(destination = "dbserver1.public.products")
        public void anotherCapture(CapturingEvent<SourceRecord> event) {
            isCapturingFilteredEvent.incrementAndGet();
        }

        public boolean isRecordChangeInvoked() {
            return isRecordChangeInvoked.getAndSet(false);
        }

        public boolean isCapturingEventInvoked() {
            return isCapturingEventInvoked.getAndSet(false);
        }

        public List<Order> getOrders() {
            return orders;
        }

        public int filteredEvent() {
            return isCapturingFilteredEvent.getAndSet(0);
        }

    }

    public record Order(int id, String name) {
    }

    static class OrderDeserializer extends ObjectMapperDeserializer<Order> {

        public OrderDeserializer() {
            super(Order.class);
        }

        public OrderDeserializer(ObjectMapper objectMapper) {
            super(Order.class, objectMapper);
        }
    }
}
