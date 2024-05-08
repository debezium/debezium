/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.connector.common.AbstractPartitionTest;

public class MongoDbPartitionTest extends AbstractPartitionTest<MongoDbPartition> {

    @Override
    protected MongoDbPartition createPartition1() {
        return new MongoDbPartition("server1", "rs1");
    }

    @Override
    protected MongoDbPartition createPartition2() {
        return new MongoDbPartition("server2", "rs2");
    }
}
