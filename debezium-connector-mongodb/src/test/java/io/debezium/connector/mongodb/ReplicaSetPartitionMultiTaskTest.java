/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.connector.common.AbstractPartitionTest;

public class ReplicaSetPartitionMultiTaskTest extends AbstractPartitionTest<ReplicaSetPartition> {

    @Override
    protected ReplicaSetPartition createPartition1() {
        return new ReplicaSetPartition("server1", "rs1", true, 0, 1, 3);
    }

    @Override
    protected ReplicaSetPartition createPartition2() {
        return new ReplicaSetPartition("server1", "rs1", true, 1, 1, 3);
    }
}
