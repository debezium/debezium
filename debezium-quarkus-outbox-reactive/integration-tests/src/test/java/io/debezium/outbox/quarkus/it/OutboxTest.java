/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import javax.inject.Inject;

import org.hibernate.reactive.mutiny.Mutiny;
import org.junit.jupiter.api.Test;

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

    // @Inject
    // EntityManager entityManager;

    @Inject
    MyService myService;

    @Inject
    Mutiny.SessionFactory sessionFactory;

    @Override
    @Test
    // @SuppressWarnings("rawtypes")
    public void firedEventGetsPersistedInOutboxTable() {
        var finished = this.myService.doSomething()
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .assertSubscribed()
                .awaitItem(Duration.ofSeconds(5))
                .getItem();
        System.out.println(finished);

        final Map row = (Map) sessionFactory.withSession(
                session -> session.createQuery("FROM OutboxEvent").getSingleResult())
                .await().indefinitely();
        // final Map row = (Map) entityManager.createQuery("FROM OutboxEvent").getSingleResult();
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
    }
}
