/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.hibernate.reactive.mutiny.Mutiny;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;

/**
 * Integration tests for the Debezium Outbox extension, using default configuration from application.properties.
 *
 * @author Chris Cranford
 */
@QuarkusTest
@TestProfile(OutboxProfiles.Default.class)
public class OutboxTest extends AbstractOutboxTest {
    @Inject
    MyService myService;

    @Inject
    Mutiny.SessionFactory sessionFactory;

    public record OutboxEvent(JsonNode key, JsonNode value) {
        public OutboxEvent {
            Objects.requireNonNull(key);
            Objects.requireNonNull(value);
        }
    }

    private static List<OutboxEvent> outboxEvents = new ArrayList<>();

    @Incoming("outbox-event")
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
        // a way to subscribe to the mock service call
        var finished = this.myService.doSomething()
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .assertSubscribed()
                .awaitItem(Duration.ofSeconds(5))
                .getItem();

        // to test jsonnode
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj;
        try {
            actualObj = mapper.readValue("{\"something\":\"Some amazing payload\"}", JsonNode.class);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        final Map row = sessionFactory.withSession(
                session -> session.createSelectionQuery("FROM OutboxEvent", Map.class).getSingleResult())
                .await().indefinitely();
        assertNotNull(row.get("id"));
        assertEquals(1L, row.get("aggregateId"));
        assertEquals("MyOutboxEvent", row.get("aggregateType"));
        assertEquals("SomeType", row.get("type"));
        assertTrue(((Instant) row.get("timestamp")).isBefore(Instant.now()));
        assertEquals(actualObj, row.get("payload"));
        assertNotNull(row.get("tracingspancontext"));
        assertEquals("John Doe", row.get("name"));
        assertEquals("JOHN DOE", row.get("name_upper"));
        assertEquals("Jane Doe", row.get("name_no_columndef"));

        await().atMost(30, TimeUnit.SECONDS).until(() -> !outboxEvents.isEmpty());
        assertAll(
                () -> assertEquals("1", outboxEvents.getFirst().key().asText()),
                () -> assertEquals("{\"something\":\"Some amazing payload\"}", outboxEvents.getFirst().value().asText()));
    }
}
