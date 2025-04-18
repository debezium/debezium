/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogDdlParserIT;

/**
 * Integration test for testing column constraints supported in MySQL 8.0.x.
 */
public class MySqlParserIT extends BinlogDdlParserIT<MySqlConnector> implements MySqlCommon {

}
