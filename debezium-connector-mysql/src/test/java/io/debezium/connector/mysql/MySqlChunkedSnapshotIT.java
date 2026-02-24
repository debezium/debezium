/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogChunkedSnapshotIT;

/**
 * MySQL-specific chunked table snapshot integration tests.
 *
 * @author Chris Cranford
 */
public class MySqlChunkedSnapshotIT extends BinlogChunkedSnapshotIT<MySqlConnector> implements MySqlCommon {

    @Override
    public Class<MySqlConnector> getConnectorClass() {
        return MySqlConnector.class;
    }

    @Override
    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());
    }

    @Override
    protected void waitForStreamingRunning() throws InterruptedException {
        waitForStreamingRunning("mysql", DATABASE.getServerName());
    }

    @Override
    protected String connector() {
        return Module.name();
    }

    @Override
    protected String server() {
        return DATABASE.getServerName();
    }
}
