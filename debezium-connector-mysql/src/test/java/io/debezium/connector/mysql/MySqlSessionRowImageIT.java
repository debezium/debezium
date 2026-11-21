/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.junit.jupiter.api.Test;

import io.debezium.connector.binlog.BinlogSessionRowImageIT;
import io.debezium.doc.FixFor;

public class MySqlSessionRowImageIT extends BinlogSessionRowImageIT<MySqlConnector> implements MySqlCommon {

    @Test
    @FixFor("debezium/dbz#962")
    void shouldFailOnUpdateAfterImageThatOmitsColumnsOfNoblobSession() throws Exception {
        // Without a primary key, the before image of an update contains all columns, while the after image omits the unchanged TEXT column
        assertFailsOnRowImageThatOmitsColumnsOfSession("NOBLOB", false, "UPDATE", "UPDATE after");
    }
}
