/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import org.junit.jupiter.api.Test;

import io.debezium.connector.binlog.BinlogReadOnlyIncrementalSnapshotIT;
import io.debezium.connector.mariadb.jdbc.MariaDbFieldReader;
import io.debezium.junit.Flaky;

/**
 * @author Chris Cranford
 */
public class ReadOnlyIncrementalSnapshotIT extends BinlogReadOnlyIncrementalSnapshotIT<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected Class<?> getFieldReader() {
        return MariaDbFieldReader.class;
    }

    /**
     * The DDL loop competes for the table metadata lock with the in-flight incremental snapshot chunk
     * reads; on a slow CI runner an ALTER can stay blocked past the 600s query timeout that MariaDB
     * Connector/J enforces via {@code SET STATEMENT max_statement_time}, killing the statement.
     */
    @Override
    @Test
    @Flaky("debezium/dbz#2604")
    public void schemaChanges() throws Exception {
        super.schemaChanges();
    }
}
