/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogReselectColumnsProcessorIT;

/**
 * @author Chris Cranford
 */
public class ReselectColumnsProcessorIT extends BinlogReselectColumnsProcessorIT<MariaDbConnector> implements MariaDbCommon {
    @Override
    public Class<MariaDbConnector> getConnectorClass() {
        return MariaDbConnector.class;
    }
}
