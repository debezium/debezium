/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2;

import org.junit.Test;

import io.debezium.connector.db2.util.TestHelper;

/**
 * Integration test for {@link Db2Connection}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class Db2ConnectionIT {

    @Test
    public void shouldEnableCdcForDatabase() throws Exception {
        try (Db2Connection connection = TestHelper.adminConnection()) {
            connection.connect();
            TestHelper.enableDbCdc(connection);
        }
    }

}
