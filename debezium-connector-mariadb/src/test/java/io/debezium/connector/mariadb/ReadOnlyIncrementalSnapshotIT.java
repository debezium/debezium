/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogReadOnlyIncrementalSnapshotIT;
import io.debezium.connector.mariadb.jdbc.MariaDbFieldReader;

/**
 * @author Chris Cranford
 */
public class ReadOnlyIncrementalSnapshotIT extends BinlogReadOnlyIncrementalSnapshotIT<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected Class<?> getFieldReader() {
        return MariaDbFieldReader.class;
    }
}
