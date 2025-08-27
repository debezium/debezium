package io.debezium.connector.postgresql.connection;

import io.debezium.connector.postgresql.PostgresConnectorConfig;

public class WALPositionLocatorFactory {

    public static PositionLocator create(PostgresConnectorConfig connectorConfig, Lsn lastCommitLsn, Lsn lsn, ReplicationMessage.Operation operation, Long lastCommitTransactionId) {
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
