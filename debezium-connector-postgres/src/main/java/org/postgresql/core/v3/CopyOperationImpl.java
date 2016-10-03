/*-------------------------------------------------------------------------
*
* Copyright (c) 2009-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core.v3;

import org.postgresql.copy.CopyOperation;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;

public abstract class CopyOperationImpl implements CopyOperation {
  QueryExecutorImpl queryExecutor;
  int rowFormat;
  int[] fieldFormats;
  long handledRowCount = -1;

  void init(QueryExecutorImpl q, int fmt, int[] fmts) {
    queryExecutor = q;
    rowFormat = fmt;
    fieldFormats = fmts;
  }

  public void cancelCopy() throws SQLException {
    queryExecutor.cancelCopy(this);
  }

  public int getFieldCount() {
    return fieldFormats.length;
  }

  public int getFieldFormat(int field) {
    return fieldFormats[field];
  }

  public int getFormat() {
    return rowFormat;
  }

  public boolean isActive() {
    synchronized (queryExecutor) {
      return queryExecutor.hasLock(this);
    }
  }

  public void handleCommandStatus(String status) throws PSQLException {
    if (status.startsWith("COPY")) {
      int i = status.lastIndexOf(' ');
      handledRowCount = i > 3 ? Long.parseLong(status.substring(i + 1)) : -1;
    } else {
      throw new PSQLException(GT.tr("CommandComplete expected COPY but got: " + status),
          PSQLState.COMMUNICATION_ERROR);
    }
  }

  /**
   * Consume received copy data
   *
   * @param data data that was receive by copy protocol
   * @throws PSQLException if some internal problem occurs
   */
  protected abstract void handleCopydata(byte[] data) throws PSQLException;

  public long getHandledRowCount() {
    return handledRowCount;
  }
}
