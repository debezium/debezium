/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogYearIT;

/**
 * Verify conversions around 2 and 4 digit year values.
 *
 * @author Jiri Pechanec
 */
public class MySqlYearIT extends BinlogYearIT<MySqlConnector> implements MySqlCommon {

}
