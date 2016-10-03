/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import java.util.Map;

/**
 * Abstraction of a generic Query, hiding the details of any protocol-version-specific data needed
 * to execute the query efficiently.
 * <p>
 * Query objects should be explicitly closed when no longer needed; if resources are allocated on
 * the server for this query, their cleanup is triggered by closing the Query.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public interface Query {
  /**
   * Create a ParameterList suitable for storing parameters associated with this Query.
   * <p>
   * If this query has no parameters, a ParameterList will be returned, but it may be a shared
   * immutable object. If this query does have parameters, the returned ParameterList is a new list,
   * unshared by other callers.
   *
   * @return a suitable ParameterList instance for this query
   */
  ParameterList createParameterList();

  /**
   * Stringize this query to a human-readable form, substituting particular parameter values for
   * parameter placeholders.
   *
   * @param parameters a ParameterList returned by this Query's {@link #createParameterList} method,
   *        or <code>null</code> to leave the parameter placeholders unsubstituted.
   * @return a human-readable representation of this query
   */
  String toString(ParameterList parameters);

  /**
   * Returns SQL in native for database format
   * @return SQL in native for database format
   */
  String getNativeSql();

  /**
   * Returns properties of the query (sql keyword, and some other parsing info).
   * @return returns properties of the query (sql keyword, and some other parsing info) or null if not applicable
   */
  SqlCommand getSqlCommand();

  /**
   * Close this query and free any server-side resources associated with it. The resources may not
   * be immediately deallocated, but closing a Query may make the deallocation more prompt.
   * <p>
   * A closed Query should not be executed.
   */
  void close();

  boolean isStatementDescribed();

  boolean isEmpty();

  /**
   * Get the number of times this Query has been batched.
   * @return number of times <code>addBatch()</code> has been called.
   */
  int getBatchSize();

  /**
   * Get a map that a result set can use to find the index associated to a name.
   *
   * @return null if the query implementation does not support this method.
   */
  Map<String, Integer> getResultSetColumnNameIndexMap();

  /**
   * Return a list of the Query objects that make up this query. If this object is already a
   * SimpleQuery, returns null (avoids an extra array construction in the common case).
   *
   * @return an array of single-statement queries, or <code>null</code> if this object is already a
   *         single-statement query.
   */
  Query[] getSubqueries();
}
