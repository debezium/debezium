/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogPartitionTest;

/**
 * @author Chris Cranford
 */
public class PartitionTest extends BinlogPartitionTest<MariaDbPartition> {
    @Override
    protected MariaDbPartition createPartition1() {
        return new MariaDbPartition("server1", "database1");
    }

    @Override
    protected MariaDbPartition createPartition2() {
        return new MariaDbPartition("server2", "database1");
    }
}
