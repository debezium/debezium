/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogFieldTest;

/**
 * @author Chris Cranford
 */
public class FieldTest extends BinlogFieldTest<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected Field.Set getAllFields() {
        return MariaDbConnectorConfig.ALL_FIELDS;
    }
}
