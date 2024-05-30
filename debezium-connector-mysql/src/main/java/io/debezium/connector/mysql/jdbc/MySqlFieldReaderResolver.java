/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.mysql.MySqlConnectorConfig;

/**
 * Used to resolve which {@link BinlogFieldReader} implementation to use for MySQL.
 *
 * @author Chris Cranford
 */
public final class MySqlFieldReaderResolver {

    private MySqlFieldReaderResolver() {
    }

    /**
     * Resolve which binlog field reader to use.
     *
     * @param connectorConfig the connector configuration; never null
     * @return the binlog field reader to use; never null
     */
    public static BinlogFieldReader resolve(MySqlConnectorConfig connectorConfig) {
        if (connectorConfig.useCursorFetch()) {
            return new MySqlBinaryProtocolFieldReader(connectorConfig);
        }
        return new MySqlTextProtocolFieldReader(connectorConfig);
    }
}
