/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorConfigTest;

/**
 * @author Chris Cranford
 */
public class ConnectorConfigTest extends BinlogConnectorConfigTest<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected MariaDbConnector getConnectorInstance() {
        return new MariaDbConnector();
    }

    @Override
    protected Field.Set getAllFields() {
        return MariaDbConnectorConfig.ALL_FIELDS;
    }
}
