/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.hibernate.QueryException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.junit.jupiter.api.Test;

import io.debezium.outbox.quarkus.internal.OutboxConstants;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration tests for the Debezium Outbox extension, disabling OpenTelemetry behavior.
 *
 * @author Chris Cranford
 */
@QuarkusTest
@TestProfile(OutboxProfiles.OpenTelemetryDisabled.class)
@QuarkusTestResource(DatabaseTestResource.class)
public class OutboxWithoutOpenTelemetryTest extends AbstractOutboxTest {

    @Test
    public void testOutboxEntityMetamodelDoesntHaveTracingSpanColumn() throws Exception {
        final MetamodelImplementor metadata = entityManager.unwrap(SessionImplementor.class).getFactory().getMetamodel();

        final EntityPersister persister = metadata.entityPersister(OutboxConstants.OUTBOX_ENTITY_FULLNAME);
        assertNotNull(persister);

        try {
            assertEquals(String.class, persister.getPropertyType("aggregateType").getReturnedClass());
            persister.getPropertyType("tracingspancontext");
            fail("Expected exception not thrown");
        }
        catch (QueryException e) {
            assertEquals("could not resolve property: tracingspancontext of: io.debezium.outbox.quarkus.internal.OutboxEvent", e.getMessage());
        }
    }
}
