package org.postgresql.replication.fluent.physical;

import org.postgresql.replication.PGReplicationStream;

import java.sql.SQLException;

public interface StartPhysicalReplicationCallback {
  PGReplicationStream start(PhysicalReplicationOptions options) throws SQLException;
}
