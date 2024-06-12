/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration tests for the Debezium Outbox extension, enabling remove-after-insert behavior.
 *
 * @author Chris Cranford
 */
@QuarkusTest
@TestProfile(OutboxProfiles.EnableRemoveAfterInsert.class)
@QuarkusTestResource(DatabaseTestResource.class)
public class OutboxWithoutRemoveAfterInsertTest extends AbstractOutboxTest {
    @Test
    @SuppressWarnings("rawtypes")
    @Override
    public void firedEventGetsPersistedInOutboxTable() {
        myService.doSomething();

        final Long count = (Long) entityManager.createQuery("SELECT COUNT(1) FROM OutboxEvent").getSingleResult();
        assertEquals(count, 0L);
    }
}
