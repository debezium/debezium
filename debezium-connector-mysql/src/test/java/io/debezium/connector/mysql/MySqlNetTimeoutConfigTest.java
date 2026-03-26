/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogNetTimeoutConfigTest;

/**
 * MySQL-specific tests for the net_write_timeout and net_read_timeout configuration properties.
 *
 * @author Jia Fan
 */
public class MySqlNetTimeoutConfigTest extends BinlogNetTimeoutConfigTest<MySqlConnector> implements MySqlCommon {

    @Override
    protected BinlogConnectorConfig createConnectorConfig(Configuration config) {
        return new MySqlConnectorConfig(config);
    }
}
