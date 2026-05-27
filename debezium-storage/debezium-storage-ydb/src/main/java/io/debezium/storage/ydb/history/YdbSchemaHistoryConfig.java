/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb.history;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.storage.ydb.YdbCommonConfig;

public class YdbSchemaHistoryConfig extends YdbCommonConfig {

    public static final Field CONNECTOR_NAME = Field.create("schema.history.internal.ydb.connector.name")
            .withDescription("Connector identifier used as PK prefix in dbz_schema_history")
            .required();

    public static final Field TABLE_NAME = Field.create("schema.history.internal.ydb.table.name")
            .withDescription("YDB table that stores schema history records")
            .withDefault("debezium/dbz_schema_history");

    private final String connectorName;
    private final String tableName;

    public YdbSchemaHistoryConfig(Configuration config) {
        super(config);
        this.connectorName = config.getString(CONNECTOR_NAME);
        this.tableName = config.getString(TABLE_NAME);
    }

    public String getConnectorName() {
        return connectorName;
    }

    public String getTableName() {
        return tableName;
    }
}