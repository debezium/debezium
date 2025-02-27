/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogCloudEventsConverterIT;

/**
 * @author Chris Cranford
 */
public class CloudEventsConverterIT extends BinlogCloudEventsConverterIT<MariaDbConnector> implements MariaDbCommon {
    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    public Class<MariaDbConnector> getConnectorClass() {
        return MariaDbConnector.class;
    }
}
