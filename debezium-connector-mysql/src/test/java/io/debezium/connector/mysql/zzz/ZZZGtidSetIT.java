/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.zzz;

import io.debezium.connector.binlog.zzz.ZZZBinlogGtidSetIT;
import io.debezium.connector.mysql.MySqlCommon;
import io.debezium.connector.mysql.MySqlConnector;

/**
 * The test is named to make sure it runs alphabetically last as it can influence execution of other tests.
 *
 * @author Jiri Pechanec
 */
public class ZZZGtidSetIT extends ZZZBinlogGtidSetIT<MySqlConnector> implements MySqlCommon {

}
