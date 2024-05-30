/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogIncrementalSnapshotIT;
import io.debezium.connector.mariadb.jdbc.MariaDbFieldReader;

/**
 * @author Chris Cranford
 */
public class IncrementalSnapshotIT extends BinlogIncrementalSnapshotIT<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected Class<?> getFieldReader() {
        return MariaDbFieldReader.class;
    }
}
