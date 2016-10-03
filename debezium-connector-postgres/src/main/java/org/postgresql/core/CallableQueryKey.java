/*-------------------------------------------------------------------------
*
* Copyright (c) 2015, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

/**
 * Serves as a cache key for {@link java.sql.CallableStatement}.
 * Callable statements require some special parsing before use (due to JDBC {@code {?= call...}}
 * syntax, thus a special cache key class is used to trigger proper parsing for callable statements.
 */
class CallableQueryKey extends BaseQueryKey {
  public CallableQueryKey(String sql) {
    super(sql, true, true);
  }

  @Override
  public String toString() {
    return "CallableQueryKey{"
        + "sql='" + sql + '\''
        + ", isParameterized=" + isParameterized
        + ", escapeProcessing=" + escapeProcessing
        + '}';
  }

  @Override
  public int hashCode() {
    return super.hashCode() * 31;
  }
}
