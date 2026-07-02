/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogMetadataBasedSchemaIT;

/**
 * MySQL integration test for the binlog-metadata-based schema mode (debezium/dbz#978).
 *
 * @author Debezium contributor
 */
public class MySqlBinlogMetadataSchemaIT extends BinlogMetadataBasedSchemaIT<MySqlConnector> implements MySqlCommon {

}
