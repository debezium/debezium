/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogReselectColumnsProcessorIT;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;

/**
 * MySQL's integration tests for {@link ReselectColumnsPostProcessor}.
 *
 * @author Chris Cranford
 */
public class MySqlReselectColumnsProcessorIT extends BinlogReselectColumnsProcessorIT<MySqlConnector> implements MySqlCommon {
    @Override
    public Class<MySqlConnector> getConnectorClass() {
        return MySqlConnector.class;
    }
}
