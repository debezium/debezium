/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.connector.common.AbstractPartitionTest;

public class SqlServerPartitionTest extends AbstractPartitionTest<SqlServerPartition> {

    @Override
    protected SqlServerPartition createPartition1() {
        return new SqlServerPartition("server1", "database1");
    }

    @Override
    protected SqlServerPartition createPartition2() {
        return new SqlServerPartition("server2", "database2");
    }
}
