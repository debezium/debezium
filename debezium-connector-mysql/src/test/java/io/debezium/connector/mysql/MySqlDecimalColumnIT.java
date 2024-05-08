/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogDecimalColumnIT;

/**
 * Tests around {@code DECIMAL} columns.
 *
 * @author Gunnar Morling
 */
public class MySqlDecimalColumnIT extends BinlogDecimalColumnIT<MySqlConnector> implements MySqlCommon {

}
