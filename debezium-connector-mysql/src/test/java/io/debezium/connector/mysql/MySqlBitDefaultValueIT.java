/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.List;

import io.debezium.connector.binlog.BinlogBitDefaultValueIT;
import io.debezium.connector.binlog.BitDefaultValueTestCases;
import io.debezium.connector.binlog.BitDefaultValueTestCases.BitDefaultValueCase;

public class MySqlBitDefaultValueIT extends BinlogBitDefaultValueIT<MySqlConnector> implements MySqlCommon {

    @Override
    protected List<BitDefaultValueCase> nonStrictBitDefaultCases() {
        return BitDefaultValueTestCases.mysqlNonStrictCases().toList();
    }
}
