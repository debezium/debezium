/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogPartitionTest;

public class MySqlPartitionTest extends BinlogPartitionTest<MySqlPartition> {
    @Override
    protected MySqlPartition createPartition1() {
        return new MySqlPartition("server1", "database1");
    }

    @Override
    protected MySqlPartition createPartition2() {
        return new MySqlPartition("server2", "database1");
    }
}
