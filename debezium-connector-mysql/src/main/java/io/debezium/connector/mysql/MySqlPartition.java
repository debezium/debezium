/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.DATABASE_NAME;

import java.util.Collections;
import java.util.Set;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogPartition;
import io.debezium.pipeline.spi.Partition;

public class MySqlPartition extends BinlogPartition {
    public MySqlPartition(String serverName, String databaseName) {
        super(serverName, databaseName);
    }

    public static class Provider implements Partition.Provider<MySqlPartition> {
        private final MySqlConnectorConfig connectorConfig;
        private final Configuration taskConfig;

        public Provider(MySqlConnectorConfig connectorConfig, Configuration taskConfig) {
            this.connectorConfig = connectorConfig;
            this.taskConfig = taskConfig;
        }

        @Override
        public Set<MySqlPartition> getPartitions() {
            return Collections.singleton(new MySqlPartition(
                    connectorConfig.getLogicalName(), taskConfig.getString(DATABASE_NAME.name())));
        }
    }
}
