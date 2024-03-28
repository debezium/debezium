/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogBlockingSnapshotIT;

/**
 * @author Chris Cranford
 */
public class BlockingSnapshotIT extends BinlogBlockingSnapshotIT<MariaDbConnector> implements MariaDbCommon {

}
