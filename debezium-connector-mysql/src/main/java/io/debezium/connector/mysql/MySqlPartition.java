/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.DATABASE_NAME;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public class MySqlPartition extends AbstractPartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public MySqlPartition(String serverName, String databaseName) {
        super(databaseName);
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final MySqlPartition other = (MySqlPartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    @Override
    public String toString() {
        return "MySqlPartition [sourcePartition=" + getSourcePartition() + "]";
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
