package org.postgresql.core.v2;


import org.postgresql.core.ReplicationProtocol;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.LogicalReplicationOptions;
import org.postgresql.replication.fluent.physical.PhysicalReplicationOptions;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

public class V2ReplicationProtocol implements ReplicationProtocol {
  public PGReplicationStream startLogical(LogicalReplicationOptions options) throws PSQLException {
    throw new PSQLException(GT.tr("ReplicationProtocol not implemented for protocol version 2"),
        PSQLState.NOT_IMPLEMENTED);
  }

  public PGReplicationStream startPhysical(PhysicalReplicationOptions options)
      throws PSQLException {
    throw new PSQLException(GT.tr("ReplicationProtocol not implemented for protocol version 2"),
        PSQLState.NOT_IMPLEMENTED);
  }
}
