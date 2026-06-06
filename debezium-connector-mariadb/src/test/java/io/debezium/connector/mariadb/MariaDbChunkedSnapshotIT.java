/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogChunkedSnapshotIT;

/**
 * MariaDB-specific chunked table snapshot integration tests.
 *
 * @author Chris Cranford
 */
public class MariaDbChunkedSnapshotIT extends BinlogChunkedSnapshotIT<MariaDbConnector> implements MariaDbCommon {

    @Override
    public Class<MariaDbConnector> getConnectorClass() {
        return MariaDbConnector.class;
    }

    @Override
    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted(connector(), server());
    }

    @Override
    protected void waitForStreamingRunning() throws InterruptedException {
        waitForStreamingRunning(connector(), server());
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
