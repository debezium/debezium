/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.binlog.BinlogReadOnlyIncrementalSnapshotIT;
import io.debezium.connector.binlog.junit.SkipTestDependingOnGtidModeRule;
import io.debezium.connector.binlog.junit.SkipWhenGtidModeIs;
import io.debezium.connector.mysql.jdbc.MySqlBinaryProtocolFieldReader;

@SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.OFF, reason = "Read only connection requires GTID_MODE to be ON")
public class ReadOnlyIncrementalSnapshotIT extends BinlogReadOnlyIncrementalSnapshotIT<MySqlConnector> implements MySqlCommon {

    @Rule
    public TestRule skipTest = new SkipTestDependingOnGtidModeRule();

    @Override
    protected Class<?> getFieldReader() {
        return MySqlBinaryProtocolFieldReader.class;
    }

    @Test(expected = ConnectException.class)
    @SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.ON, reason = "Read only connection requires GTID_MODE to be ON")
    public void shouldFailIfGtidModeIsOff() throws Exception {
        // Testing.Print.enable();
        populateTable();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        startConnector((success, message, error) -> exception.set(error));
        waitForEngineShutdown();
        stopConnector();
        final Throwable e = exception.get();
        if (e != null) {
            throw (RuntimeException) e;
        }
    }
}
