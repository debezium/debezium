package org.postgresql.core.v3;


import org.postgresql.copy.CopyDual;
import org.postgresql.util.PSQLException;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class CopyDualImpl extends CopyOperationImpl implements CopyDual {
  private Queue<byte[]> received = new LinkedList<byte[]>();

  public void writeToCopy(byte[] data, int off, int siz) throws SQLException {
    queryExecutor.writeToCopy(this, data, off, siz);
  }

  public void flushCopy() throws SQLException {
    queryExecutor.flushCopy(this);
  }

  public long endCopy() throws SQLException {
    return queryExecutor.endCopy(this);
  }

  public byte[] readFromCopy() throws SQLException {
    if (received.isEmpty()) {
      queryExecutor.readFromCopy(this, true);
    }

    return received.poll();
  }

  @Override
  public byte[] readFromCopy(boolean block) throws SQLException {
    if (received.isEmpty()) {
      queryExecutor.readFromCopy(this, block);
    }

    return received.poll();
  }

  @Override
  public void handleCommandStatus(String status) throws PSQLException {
  }

  protected void handleCopydata(byte[] data) {
    received.add(data);
  }
}
