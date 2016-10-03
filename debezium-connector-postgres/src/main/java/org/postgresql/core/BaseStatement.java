/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import org.postgresql.PGStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Driver-internal statement interface. Application code should not use this interface.
 */
public interface BaseStatement extends PGStatement, Statement {
  /**
   * Create a synthetic resultset from data provided by the driver.
   *
   * @param fields the column metadata for the resultset
   * @param tuples the resultset data
   * @return the new ResultSet
   * @throws SQLException if something goes wrong
   */
  ResultSet createDriverResultSet(Field[] fields, List<byte[][]> tuples) throws SQLException;

  /**
   * Create a resultset from data retrieved from the server.
   *
   * @param originalQuery the query that generated this resultset; used when dealing with updateable
   *        resultsets
   * @param fields the column metadata for the resultset
   * @param tuples the resultset data
   * @param cursor the cursor to use to retrieve more data from the server; if null, no additional
   *        data is present.
   * @return the new ResultSet
   * @throws SQLException if something goes wrong
   */
  ResultSet createResultSet(Query originalQuery, Field[] fields, List<byte[][]> tuples,
      ResultCursor cursor) throws SQLException;

  /**
   * Execute a query, passing additional query flags.
   *
   * @param p_sql the query to execute (JDBC-style query)
   * @param flags additional {@link QueryExecutor} flags for execution; these are bitwise-ORed into
   *        the default flags.
   * @return true if there is a result set
   * @throws SQLException if something goes wrong.
   */
  boolean executeWithFlags(String p_sql, int flags) throws SQLException;

  /**
   * Execute a query, passing additional query flags.
   *
   * @param cachedQuery the query to execute (native to PostgreSQL)
   * @param flags additional {@link QueryExecutor} flags for execution; these are bitwise-ORed into
   *        the default flags.
   * @return true if there is a result set
   * @throws SQLException if something goes wrong.
   */
  boolean executeWithFlags(CachedQuery cachedQuery, int flags) throws SQLException;

  /**
   * Execute a prepared query, passing additional query flags.
   *
   * @param flags additional {@link QueryExecutor} flags for execution; these are bitwise-ORed into
   *        the default flags.
   * @return true if there is a result set
   * @throws SQLException if something goes wrong.
   */
  boolean executeWithFlags(int flags) throws SQLException;
}
