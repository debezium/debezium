/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogIncrementalSnapshotIT;
import io.debezium.connector.mysql.jdbc.MySqlBinaryProtocolFieldReader;

public class IncrementalSnapshotIT extends BinlogIncrementalSnapshotIT<MySqlConnector> implements MySqlCommon {
    @Override
    protected Class<?> getFieldReader() {
        return MySqlBinaryProtocolFieldReader.class;
    }
}
