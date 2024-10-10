/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hibernate.persister.entity.EntityPersister;

/**
 * @author Chris Cranford
 */
public class TestAsserts {

    private TestAsserts() {
    }

    public static void assertIsType(EntityPersister persister, Class<?> type, String attributeName) {
        assertEquals(type.getName(), persister.findAttributeMapping(attributeName).getJavaType().getTypeName());
    }

    public static void assertHasNoMapping(EntityPersister persister, String attributeName) {
        assertNull(persister.findAttributeMapping(attributeName));
    }
}
