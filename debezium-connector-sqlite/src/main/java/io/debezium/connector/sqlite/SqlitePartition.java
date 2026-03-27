/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

/**
 * Describes the single partition for a SQLite database file.
 * Since SQLite is a single-file embedded database, there is always exactly one partition.
 *
 * @author Zihan Dai
 */
public class SqlitePartition extends AbstractPartition implements Partition {

    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;
    private final Map<String, String> sourcePartition;

    public SqlitePartition(String serverName, String databaseName) {
        super(databaseName);
        this.serverName = serverName;
        this.sourcePartition = Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    @Override
    public List<Map<String, String>> getSupportedFormats() {
        return Collections.singletonList(sourcePartition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SqlitePartition other = (SqlitePartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName);
    }

    @Override
    public String toString() {
        return "SqlitePartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<SqlitePartition> {
        private final SqliteConnectorConfig connectorConfig;

        Provider(SqliteConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<SqlitePartition> getPartitions() {
            return Collections.singleton(
                    new SqlitePartition(connectorConfig.getLogicalName(), connectorConfig.getDatabasePath()));
        }
    }
}
