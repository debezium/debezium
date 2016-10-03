/*-------------------------------------------------------------------------
*
* Copyright (c) 2016-2016, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

/**
 * Empty implementation of {@link ResultHandler} interface.
 * {@link SQLException#setNextException(SQLException)} has {@code O(N)} complexity,
 * so this class tracks the last exception object to speedup {@code setNextException}.
 */
public class ResultHandlerBase implements ResultHandler {
  // Last exception is tracked to avoid O(N) SQLException#setNextException just in case there
  // will be lots of exceptions (e.g. all batch rows fail with constraint violation or so)
  private SQLException firstException;
  private SQLException lastException;

  private SQLWarning firstWarning;
  private SQLWarning lastWarning;

  @Override
  public void handleResultRows(Query fromQuery, Field[] fields, List<byte[][]> tuples,
      ResultCursor cursor) {
  }

  @Override
  public void handleCommandStatus(String status, int updateCount, long insertOID) {
  }

  @Override
  public void secureProgress() {
  }

  @Override
  public void handleWarning(SQLWarning warning) {
    if (firstWarning == null) {
      firstWarning = lastWarning = warning;
      return;
    }
    lastWarning.setNextException(warning);
    lastWarning = warning;
  }

  @Override
  public void handleError(SQLException error) {
    if (firstException == null) {
      firstException = lastException = error;
      return;
    }
    lastException.setNextException(error);
    lastException = error;
  }

  @Override
  public void handleCompletion() throws SQLException {
    if (firstException != null) {
      throw firstException;
    }
  }

  @Override
  public SQLException getException() {
    return firstException;
  }

  @Override
  public SQLWarning getWarning() {
    return firstWarning;
  }
}
