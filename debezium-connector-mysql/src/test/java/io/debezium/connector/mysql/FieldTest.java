/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogFieldTest;

public class FieldTest extends BinlogFieldTest<MySqlConnector> implements MySqlCommon {
    @Override
    protected Field.Set getAllFields() {
        return MySqlConnectorConfig.ALL_FIELDS;
    }
}
