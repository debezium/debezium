/*-------------------------------------------------------------------------
*
* Copyright (c) 2015, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import org.postgresql.util.CanEstimateSize;

/**
 * Stores information on the parsed JDBC query. It is used to cut parsing overhead when executing
 * the same query through {@link java.sql.Connection#prepareStatement(String)}.
 */
public class CachedQuery implements CanEstimateSize {
  /**
   * Cache key. {@link String} or {@code org.postgresql.jdbc.CallableQueryKey}. It is assumed that
   * {@code String.valueOf(key)*2} would give reasonable estimate of the number of retained bytes by
   * given key (see {@link #getSize}).
   */
  public final Object key;
  public final Query query;
  public final boolean isFunction;
  public final boolean outParmBeforeFunc;

  private int executeCount;

  public CachedQuery(Object key, Query query, boolean isFunction, boolean outParmBeforeFunc) {
    this.key = key;
    this.query = query;
    this.isFunction = isFunction;
    this.outParmBeforeFunc = outParmBeforeFunc;
  }

  public void increaseExecuteCount() {
    if (executeCount < Integer.MAX_VALUE) {
      executeCount++;
    }
  }

  public void increaseExecuteCount(int inc) {
    int newValue = executeCount + inc;
    if (newValue > 0) { // if overflows, just ignore the update
      executeCount = newValue;
    }
  }

  /**
   * Number of times this statement has been used
   *
   * @return number of times this statement has been used
   */
  public int getExecuteCount() {
    return executeCount;
  }

  @Override
  public long getSize() {
    int queryLength = String.valueOf(key).length() * 2 /* 2 bytes per char */;
    return queryLength * 2 /* original query and native sql */
        + 100L /* entry in hash map, CachedQuery wrapper, etc */;
  }

  @Override
  public String toString() {
    return "CachedQuery{"
        + "executeCount=" + executeCount
        + ", query=" + query
        + ", isFunction=" + isFunction
        + ", outParmBeforeFunc=" + outParmBeforeFunc
        + '}';
  }
}
