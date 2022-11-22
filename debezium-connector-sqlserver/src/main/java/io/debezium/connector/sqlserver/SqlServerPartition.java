/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    private final List<Map<String, String>> supportedFormats;
    private final int hashCode;

    public SqlServerPartition(String serverName, String databaseName) {
        this(serverName, databaseName, true);
    }

    public SqlServerPartition(String serverName, String databaseName, boolean multiPartitionMode) {
        super(databaseName);
        this.serverName = serverName;

        this.sourcePartition = Collect.hashMapOf(SERVER_PARTITION_KEY, serverName, DATABASE_PARTITION_KEY, databaseName);

        // for connectors working in single-partition mode the format of a partition has been changed in Debezium 2.0,
        // the legacy/old format of the partition should still be supported along with the new format
        // the new format has precedence over the old format
        this.supportedFormats = multiPartitionMode ? Collections.singletonList(this.sourcePartition)
                : Arrays.asList(this.sourcePartition, Collect.hashMapOf(SERVER_PARTITION_KEY, serverName));

        this.hashCode = Objects.hash(serverName, databaseName);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    @Override
    public List<Map<String, String>> getSupportedFormats() {
        return supportedFormats;
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
