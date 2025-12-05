/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import org.junit.Test;

import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogSnapshotSourceIT;
import io.debezium.doc.FixFor;

/**
 * @author Chris Cranford
 */
public class SnapshotSourceIT extends BinlogSnapshotSourceIT<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected Field getSnapshotLockingModeField() {
        return MariaDbConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }

    @Override
    protected String getSnapshotLockingModeMinimal() {
        return MariaDbConnectorConfig.SnapshotLockingMode.MINIMAL.getValue();
    }

    @Override
    protected String getSnapshotLockingModeNone() {
        return MariaDbConnectorConfig.SnapshotLockingMode.NONE.getValue();
    }

    @Test
    @FixFor("DBZ-8940")
    public void shouldCompleteTheSnapshotWithZeroDateValues() throws InterruptedException {
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE zero_date_table (zero_date DATETIME);",
                "INSERT INTO zero_date_table VALUES (\"2015-00-12 00:00:00\");");

        config = simpleConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, "connector_test_ro_(.*).zero_date_table")
                .build();

        start(getConnectorClass(), config);

        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());
    }

    @Test
    @FixFor("DBZ-8940")
    public void shouldCompleteTheSnapshotWithDefaultZeroDateValue() throws InterruptedException {
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE zero_date_table (zero_date DATETIME DEFAULT '2015-00-12 00:00:00');",
                "INSERT INTO zero_date_table VALUES (NULL);");

        config = simpleConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, "connector_test_ro_(.*).zero_date_table")
                .build();

        start(getConnectorClass(), config);

        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());
    }
}
