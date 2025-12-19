/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogTaskContext;

/**
 * A state (context) associated with a MySQL task
 *
 * @author Jiri Pechanec
 *
 */
public class MySqlTaskContext extends BinlogTaskContext<MySqlDatabaseSchema> {
    public MySqlTaskContext(Configuration rawConfig, MySqlConnectorConfig config) {
        super(rawConfig, config);
    }
}
