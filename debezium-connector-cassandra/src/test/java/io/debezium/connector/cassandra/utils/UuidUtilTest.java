/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.utils;

import io.debezium.connector.cassandra.transforms.UuidUtil;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class UuidUtilTest {

    @Test
    public void testUuidUtil() {
        UUID uuid = UUID.randomUUID();
        assertEquals(uuid, UuidUtil.asUuid(UuidUtil.asBytes(uuid)));
    }
}
