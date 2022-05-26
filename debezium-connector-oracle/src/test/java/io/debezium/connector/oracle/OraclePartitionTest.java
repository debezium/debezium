/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.connector.common.AbstractPartitionTest;

public class OraclePartitionTest extends AbstractPartitionTest<OraclePartition> {

    @Override
    protected OraclePartition createPartition1() {
        return new OraclePartition("server1", "testDB");
    }

    @Override
    protected OraclePartition createPartition2() {
        return new OraclePartition("server2", "testDB");
    }
}
