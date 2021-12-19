/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

public class SqlServerPartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";
    private static final String DATABASE_PARTITION_KEY = "database";

    private final String serverName;
    private final String databaseName;
    private final Map<String, String> sourcePartition;
    private final int hashCode;

    public SqlServerPartition(String serverName, String databaseName, boolean multiPartitionMode) {
        this.serverName = serverName;
        this.databaseName = databaseName;

        this.sourcePartition = Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
        if (multiPartitionMode) {
            this.sourcePartition.put(DATABASE_PARTITION_KEY, databaseName);
        }

        this.hashCode = Objects.hash(serverName, databaseName);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    /**
     * Returns the SQL Server database name corresponding to the partition.
     */
    String getDatabaseName() {
        return databaseName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SqlServerPartition other = (SqlServerPartition) obj;
        return Objects.equals(serverName, other.serverName) && Objects.equals(databaseName, other.databaseName);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "SqlServerPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<SqlServerPartition> {
        private final SqlServerConnectorConfig connectorConfig;

        Provider(SqlServerConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<SqlServerPartition> getPartitions() {
            return Collections.singleton(new SqlServerPartition(
                    connectorConfig.getLogicalName(),
                    connectorConfig.getDatabaseName(),
                    connectorConfig.isMultiPartitionModeEnabled()));
        }
    }
}
