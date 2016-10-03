/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

/**
 * Poor man's Statement &amp; ResultSet, used for initial queries while we're still initializing the
 * system.
 */
public class SetupQueryRunner {

  private static class SimpleResultHandler extends ResultHandlerBase {
    private List<byte[][]> tuples;

    List<byte[][]> getResults() {
      return tuples;
    }

    public void handleResultRows(Query fromQuery, Field[] fields, List<byte[][]> tuples,
        ResultCursor cursor) {
      this.tuples = tuples;
    }

    public void handleWarning(SQLWarning warning) {
      // We ignore warnings. We assume we know what we're
      // doing in the setup queries.
    }
  }

  public static byte[][] run(QueryExecutor executor, String queryString,
      boolean wantResults) throws SQLException {
    Query query = executor.createSimpleQuery(queryString);
    SimpleResultHandler handler = new SimpleResultHandler();

    int flags = QueryExecutor.QUERY_ONESHOT | QueryExecutor.QUERY_SUPPRESS_BEGIN
        | QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
    if (!wantResults) {
      flags |= QueryExecutor.QUERY_NO_RESULTS | QueryExecutor.QUERY_NO_METADATA;
    }

    try {
      executor.execute(query, null, handler, 0, 0, flags);
    } finally {
      query.close();
    }

    if (!wantResults) {
      return null;
    }

    List<byte[][]> tuples = handler.getResults();
    if (tuples == null || tuples.size() != 1) {
      throw new PSQLException(GT.tr("An unexpected result was returned by a query."),
          PSQLState.CONNECTION_UNABLE_TO_CONNECT);
    }

    return tuples.get(0);
  }

}
