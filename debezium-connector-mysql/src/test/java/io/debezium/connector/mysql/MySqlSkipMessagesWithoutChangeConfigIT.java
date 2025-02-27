/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogSkipMessagesWithoutChangeConfigIT;

/**
 * Integration Tests for config skip.messages.without.change
 *
 * @author Ronak Jain
 */
public class MySqlSkipMessagesWithoutChangeConfigIT
        extends BinlogSkipMessagesWithoutChangeConfigIT<MySqlConnector>
        implements MySqlCommon {
}
