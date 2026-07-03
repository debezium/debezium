/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogMetadataBasedSchemaIT;

/**
 * MariaDB integration test for the binlog-metadata-based schema mode (debezium/dbz#978). MariaDB 10.5+
 * exposes {@code binlog_row_metadata} and emits the same FULL {@code TABLE_MAP} metadata as MySQL 8.0+,
 * so the shared logic applies unchanged.
 */
public class MariaDbBinlogMetadataSchemaIT extends BinlogMetadataBasedSchemaIT<MariaDbConnector> implements MariaDbCommon {

}
