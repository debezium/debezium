package org.postgresql.replication.fluent.logical;


import org.postgresql.replication.PGReplicationStream;

import java.sql.SQLException;

public interface StartLogicalReplicationCallback {
  PGReplicationStream start(LogicalReplicationOptions options) throws SQLException;
}
