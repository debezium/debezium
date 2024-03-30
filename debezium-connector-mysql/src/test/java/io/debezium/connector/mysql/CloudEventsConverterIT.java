/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogCloudEventsConverterIT;

/**
 * Integration test for {@link io.debezium.converters.CloudEventsConverter} with {@link MySqlConnector}
 *
 * @author Roman Kudryashov
 */
public class CloudEventsConverterIT extends BinlogCloudEventsConverterIT<MySqlConnector> implements MySqlCommon {
    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    public Class<MySqlConnector> getConnectorClass() {
        return MySqlConnector.class;
    }
}
