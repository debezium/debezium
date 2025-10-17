/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorConfigTest;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorTest extends BinlogConnectorConfigTest<MySqlConnector> implements MySqlCommon {
    @Override
    protected MySqlConnector getConnectorInstance() {
        return new MySqlConnector();
    }

    @Override
    protected Field.Set getAllFields() {
        return MySqlConnectorConfig.ALL_FIELDS;
    }
}
