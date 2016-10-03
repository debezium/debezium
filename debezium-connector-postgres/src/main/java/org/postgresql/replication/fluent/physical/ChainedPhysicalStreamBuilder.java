package org.postgresql.replication.fluent.physical;

import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.ChainedCommonStreamBuilder;

import java.sql.SQLException;

public interface ChainedPhysicalStreamBuilder extends
    ChainedCommonStreamBuilder<ChainedPhysicalStreamBuilder> {

  /**
   * Open physical replication stream
   *
   * @return not null PGReplicationStream available for fetch wal logs in binary form
   */
  PGReplicationStream start() throws SQLException;
}
