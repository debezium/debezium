/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.junit.jupiter.api.Test;

import io.debezium.outbox.quarkus.internal.OutboxConstants;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration tests for the Debezium Outbox extension, disabling OpenTelemetry behavior.
 *
 * @author Chris Cranford
 */
@QuarkusTest
@TestProfile(OutboxProfiles.OpenTelemetryDisabled.class)
public class OutboxWithoutOpenTelemetryTest extends AbstractOutboxTest {

    @Test
    public void testOutboxEntityMetamodelDoesntHaveTracingSpanColumn() throws Exception {
        final MappingMetamodelImplementor metadata = (MappingMetamodelImplementor) sessionFactory.getMetamodel();

        final EntityPersister persister = metadata.findEntityDescriptor(OutboxConstants.OUTBOX_ENTITY_FULLNAME);
        assertNotNull(persister);

        TestAsserts.assertIsType(persister, String.class, "aggregateType");
        TestAsserts.assertHasNoMapping(persister, "tracingspancontext");
    }
}
