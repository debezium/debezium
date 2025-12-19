/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.quarkus.debezium.engine.deserializer.CapturingEventDeserializerRegistry;
import io.quarkus.debezium.engine.deserializer.MutableCapturingEventDeserializerRegistry;
import io.quarkus.debezium.engine.deserializer.ObjectMapperDeserializer;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class CapturingTest {

    @Inject
    CaptureProductsHandler captureProductsHandler;

    @Inject
    CapturingEventDeserializerRegistry<SourceRecord> registry;

    @BeforeEach
    void setUp() {
        var mutableRegistry = (MutableCapturingEventDeserializerRegistry<SourceRecord>) registry;
        mutableRegistry.register("topic.inventory.orders", new OrderDeserializer());
        mutableRegistry.register("topic.inventory.users", new UserDeserializer());
    }

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(CaptureProductsHandler.class))
            .withConfigurationResource("quarkus-debezium-testsuite.properties");

    @Test
    @DisplayName("should invoke the default capture")
    void shouldInvokeDefaultCapture() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureProductsHandler.isInvoked()).isTrue());

    }

    @Test
    @DisplayName("should call the filtered by destination capture")
    void shouldInvokeFilteredByDestinationCapture() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureProductsHandler.filteredEvent()).isEqualTo(2));
    }

    @Test
    @DisplayName("should map and capture 'capturing' orders filtered by destination")
    void shouldMapAndCaptureOrdersFilteredByDestination() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureProductsHandler.getOrders()).containsExactlyInAnyOrder(
                        new Order(1, "one"),
                        new Order(2, "two")));
    }

    @Test
    @DisplayName("should map and capture users filtered by destination")
    void shouldMapAndCaptureUsersFilteredByDestination() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureProductsHandler.getUsers()).containsExactlyInAnyOrder(
                        new User(1, "giovanni", "developer"),
                        new User(2, "mario", "developer")));
    }

    @ApplicationScoped
    static class CaptureProductsHandler {
        private final AtomicBoolean isInvoked = new AtomicBoolean(false);
        private final AtomicInteger isCapturingFilteredEvent = new AtomicInteger(0);
        private final List<Order> orders = new ArrayList<>();
        private final List<User> users = new ArrayList<>();

        @Capturing()
        public void newCapture(CapturingEvent<SourceRecord> event) {
            isInvoked.set(true);
        }

        @Capturing(destination = "topic.inventory.products")
        public void anotherCapture(CapturingEvent<SourceRecord> event) {
            isCapturingFilteredEvent.incrementAndGet();
        }

        @Capturing(destination = "topic.inventory.orders")
        public void deserializedCapture(CapturingEvent<Order> event) {
            orders.add(event.record());
        }

        @Capturing(destination = "topic.inventory.users")
        public void deserialized(User user) {
            users.add(user);
        }

        public boolean isInvoked() {
            return isInvoked.getAndSet(false);
        }

        public List<Order> getOrders() {
            return orders;
        }

        public List<User> getUsers() {
            return users;
        }

        public int filteredEvent() {
            return isCapturingFilteredEvent.get();
        }

    }

    public record Order(int key, String name) {
    }

    public record User(int id, String name, String description) {

    }

    private static final ObjectMapper configuredMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static class OrderDeserializer extends ObjectMapperDeserializer<Order> {

        public OrderDeserializer() {
            super(Order.class, configuredMapper);
        }
    }

    public static class UserDeserializer extends ObjectMapperDeserializer<User> {

        public UserDeserializer() {
            super(User.class, configuredMapper);
        }
    }

}
