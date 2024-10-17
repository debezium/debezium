/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.reactive.messaging.annotations.Blocking;

/**
 * Integration tests for the Debezium Outbox extension, using default configuration from application.properties.
 *
 * @author Chris Cranford
 */
@QuarkusTest
@TestProfile(OutboxProfiles.Default.class)
public class OutboxTest extends AbstractOutboxTest {

    @Inject
    EntityManager entityManager;

    @Inject
    MyService myService;

    public record OutboxEvent(JsonNode key, JsonNode value) {
        public OutboxEvent {
            Objects.requireNonNull(key);
            Objects.requireNonNull(value);
        }
    }

    private static List<OutboxEvent> outboxEvents = new ArrayList<>();

    @Incoming("outbox-event")
    @Blocking
    void onOutboxEvent(final ConsumerRecord<JsonNode, JsonNode> event) {
        outboxEvents.add(new OutboxEvent(event.key(), event.value()));
    }

    @AfterEach
    public void tearDown() {
        outboxEvents = new ArrayList<>();
    }

    @Override
    @Test
    @SuppressWarnings("rawtypes")
    public void firedEventGetsPersistedInOutboxTable() {
        myService.doSomething();

        final Map row = (Map) entityManager.createQuery("FROM OutboxEvent").getSingleResult();
        assertNotNull(row.get("id"));
        assertEquals(1L, row.get("aggregateId"));
        assertEquals("MyOutboxEvent", row.get("aggregateType"));
        assertEquals("SomeType", row.get("type"));
        assertTrue(((Instant) row.get("timestamp")).isBefore(Instant.now()));
        assertEquals("Some amazing payload", row.get("payload"));
        assertNotNull(row.get("tracingspancontext"));
        assertEquals("John Doe", row.get("name"));
        assertEquals("JOHN DOE", row.get("name_upper"));
        assertEquals("Jane Doe", row.get("name_no_columndef"));

        await().atMost(30, TimeUnit.SECONDS).until(() -> !outboxEvents.isEmpty());
        assertAll(
                () -> assertEquals("1", outboxEvents.getFirst().key().asText()),
                () -> assertEquals("Some amazing payload", outboxEvents.getFirst().value().asText()));
    }
}
