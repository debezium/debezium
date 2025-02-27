/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogDecimalIT;

/**
 * Verify correct DECIMAL handling with different types of io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode.
 *
 * @author René Kerner
 */
public class MySqlDecimalIT extends BinlogDecimalIT<MySqlConnector> implements MySqlCommon {

}
