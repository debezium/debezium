/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.jdbc;

import java.sql.ResultSet;

/**
 * Helper class that storing result info. This handles both the ResultSet and no-ResultSet result
 * cases with a single interface for inspecting and stepping through them.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public class ResultWrapper {
  public ResultWrapper(ResultSet rs) {
    this.rs = rs;
    this.updateCount = -1;
    this.insertOID = -1;
  }

  public ResultWrapper(int updateCount, long insertOID) {
    this.rs = null;
    this.updateCount = updateCount;
    this.insertOID = insertOID;
  }

  public ResultSet getResultSet() {
    return rs;
  }

  public int getUpdateCount() {
    return updateCount;
  }

  public long getInsertOID() {
    return insertOID;
  }

  public ResultWrapper getNext() {
    return next;
  }

  public void append(ResultWrapper newResult) {
    ResultWrapper tail = this;
    while (tail.next != null) {
      tail = tail.next;
    }

    tail.next = newResult;
  }

  private final ResultSet rs;
  private final int updateCount;
  private final long insertOID;
  private ResultWrapper next;
}
