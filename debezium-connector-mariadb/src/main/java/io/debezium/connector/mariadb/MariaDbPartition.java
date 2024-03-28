/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.DATABASE_NAME;

import java.util.Collections;
import java.util.Set;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogPartition;
import io.debezium.pipeline.spi.Partition;

/**
 * Describes the partition scheme used by the MariaDB connector.
 *
 * @author Chris Cranford
 */
public class MariaDbPartition extends BinlogPartition {
    public MariaDbPartition(String serverName, String databaseName) {
        super(serverName, databaseName);
    }

    public static class Provider implements Partition.Provider<MariaDbPartition> {
        private final BinlogConnectorConfig connectorConfig;
        private final Configuration taskConfig;

        public Provider(BinlogConnectorConfig connectorConfig, Configuration taskConfig) {
            this.connectorConfig = connectorConfig;
            this.taskConfig = taskConfig;
        }

        @Override
        public Set<MariaDbPartition> getPartitions() {
            return Collections.singleton(new MariaDbPartition(
                    connectorConfig.getLogicalName(), taskConfig.getString(DATABASE_NAME.name())));
        }
    }

}
