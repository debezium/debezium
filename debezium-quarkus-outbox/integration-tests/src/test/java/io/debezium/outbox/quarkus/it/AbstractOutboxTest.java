/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import static io.debezium.outbox.quarkus.it.TestAsserts.assertIsType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.junit.jupiter.api.Test;

import io.debezium.outbox.quarkus.internal.OutboxConstants;

/**
 * Abstract base class for the Debezium Outbox extension test suite.  Each subclass implementation can
 * specify custom profiles to execute the same tests with differing behavior.
 *
 * @author Chris Cranford
 */
public abstract class AbstractOutboxTest {
    @Inject
    EntityManager entityManager;

    @Inject
    MyService myService;

    @Test
    public void testOutboxEntityMetamodelExists() throws Exception {
        final MappingMetamodelImplementor metadata = entityManager.unwrap(SessionImplementor.class).getFactory().getMappingMetamodel();

        final EntityPersister persister = metadata.findEntityDescriptor(OutboxConstants.OUTBOX_ENTITY_FULLNAME);
        assertNotNull(persister);

        // this assumes the default mapping settings, no custom converters or column types
        assertEquals(UUID.class, persister.getIdentifierType().getReturnedClass());
        assertIsType(persister, String.class, "aggregateType");
        assertIsType(persister, Long.class, "aggregateId");
        assertIsType(persister, String.class, "type");
        assertIsType(persister, Instant.class, "timestamp");
        assertIsType(persister, String.class, "payload");
        assertIsType(persister, String.class, "name");
        assertIsType(persister, String.class, "name_upper");
        assertIsType(persister, String.class, "name_no_columndef");
    }

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
        assertEquals("John Doe", row.get("name"));
        assertEquals("JOHN DOE", row.get("name_upper"));
        assertEquals("Jane Doe", row.get("name_no_columndef"));
    }
}
