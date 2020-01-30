/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import javax.inject.Inject;
import javax.persistence.EntityManager;

import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.junit.jupiter.api.Test;

import io.debezium.outbox.quarkus.internal.OutboxConstants;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * @author Chris Cranford
 */
@QuarkusTest
@QuarkusTestResource(DatabaseTestResource.class)
public class OutboxTest {
    @Inject
    EntityManager entityManager;

    @Test
    public void testOutboxEntityMetamodelExists() throws Exception {
        final MetamodelImplementor metadata = entityManager.unwrap(SessionImplementor.class).getFactory().getMetamodel();

        final EntityPersister persister = metadata.entityPersister(OutboxConstants.OUTBOX_ENTITY_FULLNAME);
        assertNotNull(persister);
    }
}
