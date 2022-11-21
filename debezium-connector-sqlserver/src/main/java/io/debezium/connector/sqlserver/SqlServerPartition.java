/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public class SqlServerPartition extends AbstractPartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";
    private static final String DATABASE_PARTITION_KEY = "database";

    private final String serverName;
    private final Map<String, String> sourcePartition;

    private final Map<String, String> fallbackPartition;
    private final int hashCode;

    public SqlServerPartition(String serverName, String databaseName) {
        this(serverName, databaseName, true);

    }

    public SqlServerPartition(String serverName, String databaseName, boolean multiPartitionMode) {

        super(databaseName);
        this.serverName = serverName;

        this.sourcePartition = Collect.hashMapOf(SERVER_PARTITION_KEY, serverName, DATABASE_PARTITION_KEY, databaseName);

        // backward compatibility needs to be enhanced for single-partition mode only
        this.fallbackPartition = multiPartitionMode ? null : Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);

        this.hashCode = Objects.hash(serverName, databaseName);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    public Map<String, String> getFallbackPartition() {
        return fallbackPartition;
    }

    /**
     * Returns the SQL Server database name corresponding to the partition.
     */
    public String getDatabaseName() {
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
            String serverName = connectorConfig.getLogicalName();
            boolean multiPartitionMode = connectorConfig.getDatabaseNames().size() > 1;

            return connectorConfig.getDatabaseNames().stream()
                    .map(databaseName -> new SqlServerPartition(serverName, databaseName, multiPartitionMode))
                    .collect(Collectors.toSet());
        }
    }
}
