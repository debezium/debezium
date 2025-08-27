/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import io.debezium.connector.postgresql.PostgresConnectorConfig;

/**
 * Factory class for creating {@link PositionLocator} instances based on the configured
 * logical replication mode.
 * <p>
 * If the replication mode is STREAMING, a {@link WalPositionLocatorStreaming} is returned;
 * otherwise, a {@link WalPositionLocator} is returned.
 * </p>
 *
 * This helps abstract away the construction logic based on configuration, making the rest of the
 * codebase cleaner and easier to maintain.
 *
 * @author Pranav Tiwari
 */

public class WALPositionLocatorFactory {

    public static PositionLocator create(PostgresConnectorConfig connectorConfig, Lsn lastCommitLsn, Lsn lsn, ReplicationMessage.Operation operation,
                                         Long lastCommitTransactionId) {
        PostgresConnectorConfig.LogicalReplicationMode logicalReplicationMode = connectorConfig.logicalReplicationMode();

        if (logicalReplicationMode.equals(PostgresConnectorConfig.LogicalReplicationMode.STREAMING)) {
            return new WalPositionLocatorStreaming(lastCommitLsn, lsn, operation, lastCommitTransactionId);
        }

        return new WalPositionLocator(lastCommitLsn, lsn, operation, lastCommitTransactionId);
    }

    public static PositionLocator create(PostgresConnectorConfig connectorConfig) {
        PostgresConnectorConfig.LogicalReplicationMode logicalReplicationMode = connectorConfig.logicalReplicationMode();

        if (logicalReplicationMode.equals(PostgresConnectorConfig.LogicalReplicationMode.STREAMING)) {
            return new WalPositionLocatorStreaming();
        }

        return new WalPositionLocator();
    }
}
