/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mysql;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.strategy.AbstractBinaryLogClientConfigurator;

/**
 * @author Chris Cranford
 */
public class MySqlBinaryLogClientConfigurator extends AbstractBinaryLogClientConfigurator {

    public MySqlBinaryLogClientConfigurator(MySqlConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

}
