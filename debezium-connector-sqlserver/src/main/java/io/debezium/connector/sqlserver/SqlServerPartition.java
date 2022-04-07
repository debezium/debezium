/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.connector.sqlserver.SqlServerConnectorConfig.DATABASE_NAME;
import static io.debezium.connector.sqlserver.SqlServerConnectorConfig.DATABASE_NAMES;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;
import io.debezium.util.LoggingContext;

public class SqlServerPartition implements Partition {
    private static final String DATABASE_PARTITION_KEY = "database";

    private final String databaseName;
    private final Map<String, String> sourcePartition;
    private final int hashCode;

    public SqlServerPartition(String databaseName, boolean multiPartitionMode) {
        this.databaseName = databaseName;

        if (multiPartitionMode) {
            this.sourcePartition = Collect.hashMapOf(DATABASE_PARTITION_KEY, databaseName);
        }
        else {
            this.sourcePartition = Collections.emptyMap();
        }

        this.hashCode = databaseName.hashCode();
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    @Override
    public Map<String, String> getLoggingContext() {
        return Collections.singletonMap(LoggingContext.DATABASE_NAME, databaseName);
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
        return Objects.equals(databaseName, other.databaseName);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "SqlServerPartition [databaseName=" + databaseName + "]";
    }

    static class Provider implements Partition.Provider<SqlServerPartition> {
        private final SqlServerConnectorConfig connectorConfig;
        private final Configuration taskConfig;

        Provider(SqlServerConnectorConfig connectorConfig, Configuration taskConfig) {
            this.connectorConfig = connectorConfig;
            this.taskConfig = taskConfig;
        }

        @Override
        public Set<SqlServerPartition> getPartitions() {
            boolean multiPartitionMode = connectorConfig.isMultiPartitionModeEnabled();

            List<String> databaseNames;

            if (multiPartitionMode) {
                databaseNames = Arrays.asList(taskConfig.getString(DATABASE_NAMES.name()).split(","));
            }
            else {
                databaseNames = Collections.singletonList(taskConfig.getString(DATABASE_NAME.name()));
            }

            return databaseNames.stream()
                    .map(databaseName -> new SqlServerPartition(databaseName, multiPartitionMode))
                    .collect(Collectors.toSet());
        }
    }
}
