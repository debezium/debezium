package org.postgresql.core;

import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.LogicalReplicationOptions;
import org.postgresql.replication.fluent.physical.PhysicalReplicationOptions;

import java.sql.SQLException;

/**
 * <p>Abstracts the protocol-specific details of physic and logic replication. <p>With each
 * connection open with replication options associate own instance ReplicationProtocol.
 */
public interface ReplicationProtocol {
  /**
   * @param options not null options for logical replication stream
   * @return not null stream instance from which available fetch wal logs that was decode by output
   * plugin
   */
  PGReplicationStream startLogical(LogicalReplicationOptions options) throws SQLException;

  /**
   * @param options not null options for physical replication stream
   * @return not null stream instance from which available fetch wal logs
   */
  PGReplicationStream startPhysical(PhysicalReplicationOptions options) throws SQLException;
}
