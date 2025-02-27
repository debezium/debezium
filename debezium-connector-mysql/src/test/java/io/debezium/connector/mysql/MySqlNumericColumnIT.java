/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogNumericColumnIT;

/**
 * Tests around {@code NUMERIC} columns.
 *
 * @author Gunnar Morling
 */
public class MySqlNumericColumnIT extends BinlogNumericColumnIT<MySqlConnector> implements MySqlCommon {

}
