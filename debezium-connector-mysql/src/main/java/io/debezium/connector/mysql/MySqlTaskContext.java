/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * A state (context) associated with a MySQL task
 *
 * @author Jiri Pechanec
 *
 */
public class MySqlTaskContext extends CdcSourceTaskContext {

    private final MySqlDatabaseSchema schema;
    private final BinaryLogClient binaryLogClient;

    public MySqlTaskContext(MySqlConnectorConfig config, MySqlDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), config.getCustomMetricTags(), schema::tableIds);
        this.schema = schema;
        this.binaryLogClient = new BinaryLogClient(config.hostname(), config.port(), config.username(), config.password());
    }

    public MySqlDatabaseSchema getSchema() {
        return schema;
    }

    public BinaryLogClient getBinaryLogClient() {
        return binaryLogClient;
    }
}
