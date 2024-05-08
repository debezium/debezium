/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogSchemaHistoryIT;

/**
 * The test to verify whether DDL is stored correctly in database schema history.
 *
 * @author Jiri Pechanec
 */
public class MySqlSchemaHistoryIT extends BinlogSchemaHistoryIT<MySqlConnector> implements MySqlCommon {

}
