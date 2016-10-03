/*-------------------------------------------------------------------------
*
* Copyright (c) 2015, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

/**
 * Contains parse flags from {@link Parser#modifyJdbcCall(String, boolean, int, int)}.
 */
public class JdbcCallParseInfo {
  private final String sql;
  private final boolean isFunction;
  private final boolean outParmBeforeFunc;

  public JdbcCallParseInfo(String sql, boolean isFunction, boolean outParmBeforeFunc) {
    this.sql = sql;
    this.isFunction = isFunction;
    this.outParmBeforeFunc = outParmBeforeFunc;
  }

  /**
   * SQL in a native for certain backend version
   *
   * @return SQL in a native for certain backend version
   */
  public String getSql() {
    return sql;
  }

  /**
   * Returns if given SQL is a function
   *
   * @return {@code true} if given SQL is a function
   */
  public boolean isFunction() {
    return isFunction;
  }

  /**
   * Returns if given SQL is a function with one out parameter
   *
   * @return true if given SQL is a function with one out parameter
   */
  public boolean isOutParmBeforeFunc() {
    return outParmBeforeFunc;
  }
}
