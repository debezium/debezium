/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.List;

import io.debezium.connector.binlog.BinlogBitDefaultValueIT;
import io.debezium.connector.binlog.BitDefaultValueTestCases;
import io.debezium.connector.binlog.BitDefaultValueTestCases.BitDefaultValueCase;

public class MariaDbBitDefaultValueIT extends BinlogBitDefaultValueIT<MariaDbConnector> implements MariaDbCommon {

    @Override
    protected List<BitDefaultValueCase> nonStrictBitDefaultCases() {
        return BitDefaultValueTestCases.mariaDbNonStrictCases().toList();
    }
}
