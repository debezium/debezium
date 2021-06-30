/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

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

    @Override
    @Test
    public void firedEventGetsPersistedInOutboxTable() {
        myService.doSomething();

        Query q = entityManager
                .createNativeQuery("SELECT CAST(id as varchar), aggregateId, aggregateType, type, timestamp, payload, tracingspancontext FROM OutboxEvent");
        Object[] row = (Object[]) q.getSingleResult();

        assertNotNull(UUID.fromString((String) row[0]));
        assertEquals(BigInteger.valueOf(1L), row[1]);
        assertEquals("MyOutboxEvent", row[2]);
        assertEquals("SomeType", row[3]);
        assertTrue(((Timestamp) row[4]).toInstant().isBefore(Instant.now()));
        assertEquals("Some amazing payload", row[5]);
        assertNotNull(row[6]);
    }
}
