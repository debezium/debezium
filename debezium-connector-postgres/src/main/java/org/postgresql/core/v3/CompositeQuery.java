/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core.v3;

import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.SqlCommand;

import java.util.Map;

/**
 * V3 Query implementation for queries that involve multiple statements. We split it up into one
 * SimpleQuery per statement, and wrap the corresponding per-statement SimpleParameterList objects
 * in a CompositeParameterList.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class CompositeQuery implements Query {
  CompositeQuery(SimpleQuery[] subqueries, int[] offsets) {
    this.subqueries = subqueries;
    this.offsets = offsets;
  }

  public ParameterList createParameterList() {
    SimpleParameterList[] subparams = new SimpleParameterList[subqueries.length];
    for (int i = 0; i < subqueries.length; ++i) {
      subparams[i] = (SimpleParameterList) subqueries[i].createParameterList();
    }
    return new CompositeParameterList(subparams, offsets);
  }

  public String toString(ParameterList parameters) {
    StringBuilder sbuf = new StringBuilder(subqueries[0].toString());
    for (int i = 1; i < subqueries.length; ++i) {
      sbuf.append(';');
      sbuf.append(subqueries[i]);
    }
    return sbuf.toString();
  }

  @Override
  public String getNativeSql() {
    StringBuilder sbuf = new StringBuilder(subqueries[0].toString());
    for (int i = 1; i < subqueries.length; ++i) {
      sbuf.append(';');
      sbuf.append(subqueries[i].getNativeSql());
    }
    return sbuf.toString();
  }

  @Override
  public SqlCommand getSqlCommand() {
    return null;
  }

  public String toString() {
    return toString(null);
  }

  public void close() {
    for (SimpleQuery subquery : subqueries) {
      subquery.close();
    }
  }

  public Query[] getSubqueries() {
    return subqueries;
  }

  public boolean isStatementDescribed() {
    for (SimpleQuery subquery : subqueries) {
      if (!subquery.isStatementDescribed()) {
        return false;
      }
    }
    return true;
  }

  public boolean isEmpty() {
    for (SimpleQuery subquery : subqueries) {
      if (!subquery.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  public int getBatchSize() {
    return 0; // no-op, unsupported
  }

  @Override
  public Map<String, Integer> getResultSetColumnNameIndexMap() {
    return null; // unsupported
  }

  private final SimpleQuery[] subqueries;
  private final int[] offsets;
}
