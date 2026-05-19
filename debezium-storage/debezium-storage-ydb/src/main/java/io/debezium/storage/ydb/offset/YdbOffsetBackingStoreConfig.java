/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb.offset;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.storage.ydb.YdbCommonConfig;

public class YdbOffsetBackingStoreConfig extends YdbCommonConfig {

    public static final Field CONNECTOR_NAME = Field.create("offset.storage.ydb.connector.name")
            .withDescription("Connector identifier used as the PK prefix in dbz_offsets")
            .required();

    public static final Field TABLE_NAME = Field.create("offset.storage.ydb.table.name")
            .withDescription("YDB table that stores Connect offsets")
            .withDefault("debezium/dbz_offsets");

    private final String connectorName;
    private final String tableName;

    public YdbOffsetBackingStoreConfig(Configuration config) {
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