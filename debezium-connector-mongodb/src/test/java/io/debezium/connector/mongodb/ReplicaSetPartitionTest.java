/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.connector.common.AbstractPartitionTest;

public class ReplicaSetPartitionTest extends AbstractPartitionTest<ReplicaSetPartition> {

    @Override
    protected ReplicaSetPartition createPartition1() {
        return new ReplicaSetPartition("server1", "rs1", false, -1, -1, 1);
    }

    @Override
    protected ReplicaSetPartition createPartition2() {
        return new ReplicaSetPartition("server2", "rs2", false, -1, -1, 1);
    }
}
