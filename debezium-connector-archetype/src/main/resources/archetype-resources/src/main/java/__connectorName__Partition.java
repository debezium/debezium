/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.Map;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;

/**
 * Identifies the offset partition for a ${connectorName} connector instance.
 *
 * <p>Each connector instance writes its offsets under this partition key in Kafka Connect's
 * offset storage. Connector instances with different {@code topic.prefix} values keep
 * independent offsets.
 */
public class ${connectorName}Partition implements Partition {

    private static final String SERVER_KEY = "server";

    private final String serverName;

    public ${connectorName}Partition(String serverName) {
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Map.of(SERVER_KEY, serverName);
    }

    public static class Provider implements Partition.Provider<${connectorName}Partition> {

        private final ${connectorName}ConnectorConfig config;

        public Provider(${connectorName}ConnectorConfig config) {
            this.config = config;
        }

        @Override
        public Set<${connectorName}Partition> getPartitions() {
            return Set.of(new ${connectorName}Partition(config.getLogicalName()));
        }
    }
}
