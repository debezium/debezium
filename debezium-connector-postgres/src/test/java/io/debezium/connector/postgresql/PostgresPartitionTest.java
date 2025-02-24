/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.connector.common.AbstractPartitionTest;

public class PostgresPartitionTest extends AbstractPartitionTest<PostgresPartition> {

    @Override
    protected PostgresPartition createPartition1() {
        return new PostgresPartition("server1", "database1", "0");
    }

    @Override
    protected PostgresPartition createPartition2() {
        return new PostgresPartition("server2", "database1", "0");
    }
}
