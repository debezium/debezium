/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

/**
 * The source partition used by the TiDB connector. A single connector instance captures one
 * TiDB cluster (through one TiCDC changefeed), so there is exactly one partition keyed by the
 * logical server name.
 *
 * @author Aviral Srivastava
 */
public class TiDbPartition implements Partition {

    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public TiDbPartition(String serverName) {
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
        final TiDbPartition other = (TiDbPartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName);
    }

    @Override
    public String toString() {
        return "TiDbPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    public static class Provider implements Partition.Provider<TiDbPartition> {

        private final TiDbConnectorConfig connectorConfig;

        public Provider(TiDbConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<TiDbPartition> getPartitions() {
            return Collections.singleton(new TiDbPartition(connectorConfig.getLogicalName()));
        }
    }
}
